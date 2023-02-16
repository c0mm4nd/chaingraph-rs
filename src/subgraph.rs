use std::{fs::File, result};

use crate::utils;
use indradb::{
    AllEdgeQuery, Database, Edge, IncludeQuery, QueryExt, QueryOutputValue, RocksdbDatastore,
    SpecificEdgeQuery, SpecificVertexQuery, Vertex,
};
use uuid::Uuid;

pub fn gen(datastore: Database<RocksdbDatastore>, v: Vec<String>, hop: usize, output: String) {
    // TODO

    // convert v to ids
    let ids: Vec<Uuid> = v.iter().map(|addr| utils::addr_to_uuid(addr)).collect();

    let mut output = csv::Writer::from_path("subgraph.csv").unwrap();

    let q = SpecificVertexQuery::new(ids.clone());
    let result = datastore.get(q).unwrap();
    log::debug!("{:?}", result);
    for out_val in result {
        match out_val {
            QueryOutputValue::Vertices(vertices) => {
                for v in vertices {
                    run_hop(&datastore, &mut output, hop, v)
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
) {
    if hop == 0 {
        return;
    };
    log::debug!("{:?}", v);
    let mut next_hop_vertices: Vec<Vertex> = Vec::new();

    let out_q = SpecificVertexQuery::single(v.id.clone())
        .clone()
        .outbound()
        .unwrap();
    let out_e = datastore.get(out_q).unwrap();

    for (index, edges_list) in out_e.iter().enumerate() {
        let from = v.t.as_str();

        match edges_list {
            QueryOutputValue::Edges(edges) => {
                log::debug!("{} has {} outbound edges", from, edges.len());

                let mut txs = Vec::new();
                let mut next_v = Vec::new();
                for e in edges {
                    assert!(e.outbound_id == v.id, "{:?} != {:?}", e, v.id);
                    txs.push(e.t.as_str());
                    next_v.push(e.inbound_id);
                }
                let result = datastore.get(SpecificVertexQuery::new(next_v)).unwrap();
                let result = &result[0]; // must be 1 len

                match result {
                    QueryOutputValue::Vertices(tos) => {
                        log::debug!("{} result has {} tos", from, tos.len());
                        for (j, to) in tos.iter().enumerate() {
                            output
                                .write_record(&[
                                    from.clone(),
                                    &to.t.as_str().to_owned(),
                                    &txs[j].to_owned(),
                                ])
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

    let in_q = SpecificVertexQuery::single(v.id.clone())
        .clone()
        .inbound()
        .unwrap();
    let in_e = datastore.get(in_q).unwrap();
    log::debug!("{:?} has {} inbounds", v.id, in_e.len());
    for (index, edges_list) in in_e.iter().enumerate() {
        let to = v.t.as_str();

        match edges_list {
            QueryOutputValue::Edges(edges) => {
                log::debug!("{} has {} inbound edges", to, edges.len());

                let mut txs = Vec::new();
                let mut next_v = Vec::new();
                for e in edges {
                    assert!(e.inbound_id == v.id);
                    txs.push(e.t.as_str());
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
                                    to.clone().to_owned(),
                                    txs[j].to_owned(),
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

    for next_v in next_hop_vertices {
        run_hop(datastore, output, hop - 1, next_v)
    }
}
