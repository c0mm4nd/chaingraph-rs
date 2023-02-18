use std::fs::File;

use crate::utils;
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
    opts: &Options,
    v: Vec<String>,
    hop: usize,
    output: String,
    graph_type: GraphType,
) {
    let datastore = RocksdbDatastore::new_db_with_options(path, opts).unwrap();
    // convert v to ids
    let ids: Vec<Uuid> = v.iter().map(|addr| utils::addr_to_uuid(addr)).collect();

    let mut output = csv::Writer::from_path(output).unwrap();

    let q = SpecificVertexQuery::new(ids);
    let result = datastore.get(q).unwrap();
    log::debug!("{:?}", result);
    for out_val in result {
        match out_val {
            QueryOutputValue::Vertices(vertices) => {
                let parents = vertices.clone();
                for v in vertices {
                    match graph_type {
                        GraphType::CsvAdj => run_hop(&datastore, &mut output, hop, v, &parents),
                        _ => todo!()
                    }
                }
            }
            _ => todo!(),
        }
    }
}

fn run_hop(
    datastore: &Database<RocksdbDatastore>,
    output: &mut csv::Writer<File>,
    hop: usize,
    v: Vertex,
    parents: &[Vertex],
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

        match edges_list {
            QueryOutputValue::Edges(edges) => {
                log::debug!("{} has {} outbound edges", from, edges.len());

                let mut txs = Vec::new();
                let mut next_v = Vec::new();
                for e in edges {
                    assert!(e.outbound_id == v.id, "{:?} != {:?}", e, v.id);
                    txs.push(e.t);
                    next_v.push(e.inbound_id);
                }
                let result = datastore.get(SpecificVertexQuery::new(next_v)).unwrap();
                let result = &result[0]; // must be 1 len

                match result {
                    QueryOutputValue::Vertices(tos) => {
                        log::debug!("{} result has {} tos", from, tos.len());
                        for (j, to) in tos.iter().enumerate() {
                            output
                                .write_record([from, to.t.as_str(), &txs[j].to_string()])
                                .unwrap();
                        }
                        next_hop_vertices.extend(tos.clone());
                    }
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }
    }

    let in_q = SpecificVertexQuery::single(v.id).inbound().unwrap();
    let in_e = datastore.get(in_q).unwrap();
    log::debug!("{:?} has {} inbounds", v.id, in_e.len());
    for edges_list in in_e {
        let to = v.t.as_str();

        match edges_list {
            QueryOutputValue::Edges(edges) => {
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

                match result {
                    QueryOutputValue::Vertices(froms) => {
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
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }
    }

    let mut next_parents = parents.to_vec();
    next_parents.extend(next_hop_vertices.clone());

    for next_v in next_hop_vertices {
        if !parents.contains(&next_v) {
            run_hop(datastore, output, hop - 1, next_v, &next_parents)
        }
    }
}
