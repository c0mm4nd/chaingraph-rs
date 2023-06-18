use std::{fs::File, str::FromStr, io::Write};

use crate::{utils, eth_common::TransactionInfo};
use bigdecimal::ToPrimitive;
use ethers::utils::WEI_IN_ETHER;
use hashbrown::HashSet;
use indradb::{
    Database, Identifier, QueryExt, QueryOutputValue, RocksdbDatastore, SpecificVertexQuery, Vertex, SpecificEdgeQuery,
};
use rand::seq::SliceRandom;
use rocksdb::Options;
use uuid::Uuid;

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum GraphType {
    CsvEdgelist,
    // CsvNodes, // TODO
    // Rdf, // TODO
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum VType {
    ETHAddress,
    String,
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq)]
pub enum Direction {
    Out,
    In,
    Both,
}

pub fn gen_subgraph(
    path: String,
    opts: &mut Options,
    v: &mut Vec<String>,
    hop: usize,
    output: String,
    graph_type: GraphType,
    v_type: VType,
    direction: Direction,
    with_props: Vec<String>,
) {
    opts.optimize_for_point_lookup(0x100000000);
    opts.set_optimize_filters_for_hits(true);
    opts.optimize_level_style_compaction(0x100000000);
    opts.set_memtable_whole_key_filtering(true);

    let datastore = RocksdbDatastore::new_db_with_options(path, opts).unwrap();
    // convert v to ids
    v.sort();
    v.dedup();
    let ids: Vec<Uuid> = v
        .iter()
        .map(|addr| match v_type {
            VType::ETHAddress => utils::addr_to_uuid(addr),
            VType::String => utils::str_to_uuid(addr),
        })
        .collect();
    log::debug!("{} addresses", ids.len());

    if with_props.len() > 0 {
        log::debug!("props: {:?}", with_props);
    }

    // let mut output = csv::Writer::from_path(output).unwrap();
    let mut output = File::create(output).unwrap();

    let q = SpecificVertexQuery::new(ids);
    let result = datastore.get(q).unwrap();

    for out_val in result {
        if let QueryOutputValue::Vertices(vertices) = out_val {
            log::debug!("{} vertices", vertices.len());

            let mut crawled_edges: HashSet<Identifier> = HashSet::new();
            let mut crawled_vertices: HashSet<Identifier> = HashSet::new();

            for v in &vertices {
                match graph_type {
                    GraphType::CsvEdgelist => run_hop(
                        &datastore,
                        &mut output,
                        hop,
                        v,
                        &mut crawled_edges,
                        &mut crawled_vertices,
                        direction,
                        &with_props,
                    ),
                    _ => todo!(),
                }
            }
        }
    }
}

fn run_hop(
    db: &Database<RocksdbDatastore>,
    output: &mut File,
    hop: usize,
    v: &Vertex,
    crawled_edges: &mut HashSet<Identifier>,
    crawled_vertices: &mut HashSet<Identifier>,
    direction: Direction,
    with_props: &Vec<String>,
) {
    if hop == 0 {
        return;
    };
    // log::debug!("{:?}", v);
    let mut next_hop_vertices: Vec<Vertex> = Vec::new();

    if direction == Direction::Both || direction == Direction::Out {
        let out_q = SpecificVertexQuery::single(v.id).outbound().unwrap();
        let out_e = db.get(out_q).unwrap();

        for edges_list in out_e {
            let from = v.t.as_str();

            if let QueryOutputValue::Edges(edges) = edges_list {
                log::debug!("hop {}:  {} has {} outbound edges", hop, from, edges.len());

                for e in edges {
                    // .choose_multiple(&mut rng, max_tx) {
                    assert!(e.outbound_id == v.id, "{:?} != {:?}", e, v.id);

                    if crawled_edges.contains(&e.t) {
                        continue;
                    }
                    crawled_edges.insert(e.t);

                    let result = db
                        .get(SpecificVertexQuery::single(e.inbound_id))
                        .unwrap();
                    let result = &result[0]; // must be 1 len

                    if let QueryOutputValue::Vertices(tos) = result {
                        let to = &tos[0];
                        if with_props.len() > 0 {
                            let q = SpecificEdgeQuery::single(e.clone()).properties().unwrap();
                            let properties =
                                indradb::util::extract_edge_properties(db.get(q).unwrap()).unwrap();
                            let property = &properties[0].props[0];
                            let val = property.value.0.as_ref();
                            let tx: TransactionInfo = serde_json::from_value(val.clone()).unwrap();
                            let mut attrs = serde_json::Map::with_capacity(with_props.len());
                            attrs.insert("block_number".to_owned(), tx.block_number.unwrap().as_u64().into());
                            attrs.insert("value".to_owned(),  (utils::u256_to_bigdecimal(tx.value) / utils::u256_to_bigdecimal(WEI_IN_ETHER)).to_f64().into());
                            attrs.insert("gas".to_owned(),  (utils::u256_to_bigdecimal(tx.gas) / utils::u256_to_bigdecimal(WEI_IN_ETHER)).to_f64().into());
                            attrs.insert("gas_price".to_owned(),  (utils::u256_to_bigdecimal(tx.gas) / utils::u256_to_bigdecimal(WEI_IN_ETHER)).to_f64().into());
                            attrs.insert("gas_used".to_owned(),  (utils::u256_to_bigdecimal(tx.gas_used.unwrap()) / utils::u256_to_bigdecimal(WEI_IN_ETHER)).to_f64().into());

                            // output.write_record([from, to.t.as_str(), serde_json::to_string(&attrs).unwrap().as_str()]).unwrap();
                            output.write_all((vec![from, &to.t,  serde_json::to_string(&attrs).unwrap().as_str()].join(" ") + "\n").as_bytes()).unwrap();
                        } else {
                            // output.write_record([from, to.t.as_str(), ]).unwrap();
                            output.write_all((vec![from, &to.t,  &e.t].join(" ") + "\n").as_bytes()).unwrap();
                        }

                        next_hop_vertices.push(to.to_owned());
                    }
                }
            }
        }
    }

    if direction == Direction::Both || direction == Direction::In {
        let in_q = SpecificVertexQuery::single(v.id).inbound().unwrap();
        let in_e = db.get(in_q).unwrap();

        for edges_list in in_e {
            let to = v.t.as_str();

            if let QueryOutputValue::Edges(edges) = edges_list {
                log::debug!("hop {}:  {} has {} inbound edges", hop, to, edges.len());

                for e in edges {
                    //.choose_multiple(&mut rng, max_tx) {
                    assert!(e.inbound_id == v.id);

                    if crawled_edges.contains(&e.t) {
                        continue;
                    }
                    crawled_edges.insert(e.t);

                    let result = db
                        .get(SpecificVertexQuery::single(e.outbound_id))
                        .unwrap();
                    let result = &result[0];

                    if let QueryOutputValue::Vertices(froms) = result {
                        let from = &froms[0]; // must only one

                        if with_props.len() > 0 {
                            let q = SpecificEdgeQuery::single(e.clone()).properties().unwrap();
                            let properties =
                                indradb::util::extract_edge_properties(db.get(q).unwrap()).unwrap();
                            let property = &properties[0].props[0];
                            let val = property.value.0.as_ref();
                            let tx: TransactionInfo = serde_json::from_value(val.clone()).unwrap();
                            let mut attrs = serde_json::Map::with_capacity(with_props.len());
                            attrs.insert("blockNumber".to_owned(), tx.block_number.unwrap().as_u64().into());
                            attrs.insert("value".to_owned(),  (utils::u256_to_bigdecimal(tx.value) / utils::u256_to_bigdecimal(WEI_IN_ETHER)).to_f64().into());
                            attrs.insert("gas".to_owned(),  (utils::u256_to_bigdecimal(tx.gas) / utils::u256_to_bigdecimal(WEI_IN_ETHER)).to_f64().into());
                            attrs.insert("gas_price".to_owned(),  (utils::u256_to_bigdecimal(tx.gas) / utils::u256_to_bigdecimal(WEI_IN_ETHER)).to_f64().into());
                            attrs.insert("gas_used".to_owned(),  (utils::u256_to_bigdecimal(tx.gas_used.unwrap()) / utils::u256_to_bigdecimal(WEI_IN_ETHER)).to_f64().into());
                            
                            // output.write_record([from.t.as_str(), to, serde_json::to_string(&attrs).unwrap().as_str()]).unwrap();
                            output.write_all((vec![from.t.as_str() , to,  serde_json::to_string(&attrs).unwrap().as_str()].join(",") + "\n").as_bytes()).unwrap();
                        } else {
                            // output.write_record([from.t.as_str(), to, &e.t]).unwrap();
                            output.write_all((vec![from.t.as_str() , to, &e.t].join(",") + "\n").as_bytes()).unwrap();
                        } 

                        next_hop_vertices.push(from.to_owned());
                    }
                }
            }
        }
    }

    for next_v in next_hop_vertices {
        if crawled_vertices.contains(&next_v.t) {
            continue;
        }
        crawled_vertices.insert(next_v.t);
        run_hop(
            db,
            output,
            hop - 1,
            &next_v,
            crawled_edges,
            crawled_vertices,
            direction,
            with_props,
        );
    }
}
