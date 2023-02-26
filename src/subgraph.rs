use std::fs::File;

use crate::utils;
use hashbrown::HashSet;
use indradb::{
    Database, QueryExt, QueryOutputValue, RocksdbDatastore, SpecificVertexQuery, Vertex,
};
use rocksdb::Options;
use uuid::Uuid;

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum GraphType {
    CsvAdj,
    CsvNodes,
    Rdf,
}

pub fn gen_subgraph(
    path: String,
    opts: &mut Options,
    v: &mut Vec<String>,
    hop: usize,
    output: String,
    graph_type: GraphType,
) {
    opts.optimize_for_point_lookup(0x100000000);
    opts.set_optimize_filters_for_hits(true);
    opts.optimize_level_style_compaction(0x100000000);
    opts.set_memtable_whole_key_filtering(true);

    let datastore = RocksdbDatastore::new_db_with_options(path, opts).unwrap();
    // convert v to ids
    v.sort();
    v.dedup();
    let ids: Vec<Uuid> = v.iter().map(|addr| utils::addr_to_uuid(addr)).collect();
    log::debug!("{} addresses", ids.len());

    let mut output = csv::Writer::from_path(output).unwrap();

    let q = SpecificVertexQuery::new(ids);
    let result = datastore.get(q).unwrap();

    for out_val in result {
        if let QueryOutputValue::Vertices(vertices) = out_val {
            let mut crawled = HashSet::new();
            for v in vertices.clone() {
                crawled.insert(v);
            }

            for v in &crawled.clone() {
                match graph_type {
                    GraphType::CsvAdj => run_hop(&datastore, &mut output, hop, v, &mut crawled),
                    _ => todo!(),
                }
            }
        }
    }
}

fn run_hop(
    datastore: &Database<RocksdbDatastore>,
    output: &mut csv::Writer<File>,
    hop: usize,
    v: &Vertex,
    crawled: &mut HashSet<Vertex>,
) {
    if hop == 0 {
        return;
    };
    log::debug!("{:?}", v);
    let mut next_hop_vertices: Vec<Vertex> = Vec::new();

    let out_q = SpecificVertexQuery::single(v.id).outbound().unwrap();
    let out_e = datastore.get(out_q).unwrap();

    for edges_list in out_e {
        let from = v.t.as_str();

        if let QueryOutputValue::Edges(edges) = edges_list {
            log::debug!("hop {}:  {} has {} outbound edges", hop, from, edges.len());

            for e in edges {
                assert!(e.outbound_id == v.id, "{:?} != {:?}", e, v.id);

                let result = datastore
                    .get(SpecificVertexQuery::single(e.inbound_id))
                    .unwrap();
                let result = &result[0]; // must be 1 len

                if let QueryOutputValue::Vertices(tos) = result {
                    log::debug!("{} result has {} tos", from, tos.len());
                    for to in tos {
                        output.write_record([from, to.t.as_str(), &e.t]).unwrap();
                    }
                    next_hop_vertices.extend(tos.clone());
                }
            }
        }
    }

    let in_q = SpecificVertexQuery::single(v.id).inbound().unwrap();
    let in_e = datastore.get(in_q).unwrap();
    log::debug!("{:?} has {} inbounds", v.id, in_e.len());
    for edges_list in in_e {
        let to = v.t.as_str();

        if let QueryOutputValue::Edges(edges) = edges_list {
            log::debug!("{} has {} inbound edges", to, edges.len());

            let mut txs = Vec::new();
            let mut next_v = Vec::new();
            for e in edges {
                assert!(e.inbound_id == v.id);
                txs.push(e.t);
                next_v.push(e.outbound_id);
            }
            let result = datastore.get(SpecificVertexQuery::new(next_v)).unwrap();
            log::debug!("{} has {} results", to, result.len());
            let result = &result[0];

            if let QueryOutputValue::Vertices(froms) = result {
                log::debug!("{} result has {} froms", to, froms.len());
                for (j, from) in froms.iter().enumerate() {
                    output
                        .write_record(&[
                            from.t.as_str().to_owned(),
                            to.to_string(),
                            txs[j].to_string(),
                        ])
                        .unwrap();
                }
                next_hop_vertices.extend(froms.clone());
            }
        }
    }

    for next_v in next_hop_vertices {
        if !crawled.contains(&next_v) {
            crawled.insert(next_v.clone());
            run_hop(datastore, output, hop - 1, &next_v, crawled);
        }
    }
}
