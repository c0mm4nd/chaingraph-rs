use crossbeam::deque::{Worker, Steal, Stealer};
use indradb::{
    Datastore, EdgeKey, Identifier, MemoryDatastore, SpecificEdgeQuery, SpecificVertexQuery, Vertex, RocksdbDatastore,
};
use uuid::Uuid;
use std::{fs::File, io::{self, BufRead}, collections::HashMap, str::FromStr, thread, time::Duration};
use ethers::prelude::*;

// #[tokio::main]
fn main() {
    // Create an in-memory datastore
    let mut datastore = RocksdbDatastore::new("rocks", None).unwrap();

    // Create a couple of vertices
    type Record = HashMap<String, String>;
    
    let mut reader = csv::Reader::from_path("/mnt/hdd4t/chaingraph/nodes_nodup_0-16200000.csv").unwrap();
    for (index, result) in reader.deserialize().enumerate() {
        let record: Record = result.unwrap(); 
        let addr = &record["addr:ID"];

        let v = Vertex::with_id(addr_to_uuid(addr.as_str()), Identifier::new(addr).unwrap());
        let ok = datastore.create_vertex(&v).unwrap();
        assert!(ok);
        // println!("{}: {:?}", index, record);
        println!("pushed addr: #{}", index);
    }

    let mut reader = csv::Reader::from_path("txs_0-16200000.out").unwrap();
    for (index, result) in reader.deserialize().enumerate() {
        let record: Record = result.unwrap();
        let from = &record["from"];
        let to = &record["to"];
        let hash = &record["hash"];
        // let block_num_hexstr = &record["block_number"];

        let edge = EdgeKey::new(addr_to_uuid(from), Identifier::new(hash).unwrap(), addr_to_uuid(to));
        let ok = datastore.create_edge(&edge).unwrap();
        assert!(ok);

        println!("pushed edge #{}", index);
    }

    // let v = Vertex::new(Identifier::new("").unwrap());
    // datastore.create_vertex(&out_v).unwrap();
    // datastore.create_vertex(&in_v).unwrap();

    // // Add an edge between the vertices
    // let key = EdgeKey::new(out_v.id, Identifier::new("likes").unwrap(), in_v.id);
    // datastore.create_edge(&key).unwrap();

    // // Query for the edge
    // let e = datastore
    //     .get_edges(SpecificEdgeQuery::single(key.clone()).into())
    //     .unwrap();
    // assert_eq!(e.len(), 1);
    // assert_eq!(key, e[0].key);
}

fn addr_to_uuid(addr: &str) -> Uuid {
    let address = Address::from_str(addr).unwrap();
    let id = Uuid::new_v5(&Uuid::NAMESPACE_OID, address.as_bytes());
    return id;
}