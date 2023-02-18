use std::collections::HashMap;

use crate::utils;
use indradb::{
    AllEdgeQuery, BulkInsertItem, CountQueryExt, Edge, Identifier, QueryOutputValue,
    RocksdbDatastore, Vertex,
};
use rocksdb::Options;

type Record = HashMap<String, String>;

pub fn bulk_insert(path: String, opts: &Options, csv: String, mut fail: usize, bulk: usize) {
    let datastore = RocksdbDatastore::new_db_with_options(path, opts).unwrap();
    log::warn!("start bulk insert");

    let e_count: usize = match datastore.get(AllEdgeQuery.count().unwrap()).unwrap()[0] {
        QueryOutputValue::Count(count) => count.try_into().unwrap(),
        _ => todo!(),
    };
    log::warn!("all edge: {:?}", e_count);

    if fail == 0 {
        fail = e_count
    };

    let trigger = bulk - 1;

    let mut items = Vec::new();
    let mut reader = csv::Reader::from_path(csv).unwrap();

    for (index, result) in reader.deserialize().enumerate() {
        if index < fail {
            continue;
        }

        let record: Record = result.unwrap();
        job(record, &mut items);
        if index % bulk == trigger {
            datastore.bulk_insert(items).unwrap();
            items = Vec::new();
            // datastore.sync().unwrap();
            log::warn!("bulk inserted edge #{} -> #{}", index - 999, index);
            // println!("{}", statistics.get_statistics().unwrap())
        }
    }

    datastore.bulk_insert(items).unwrap();
    log::warn!("bulk insert done, start compacting");

    datastore.sync().unwrap();
    log::warn!("everything done");
}

fn job(record: Record, items: &mut Vec<BulkInsertItem>) {
    let from = &record["from"];
    let to = &record["to"];
    let hash = &record["hash"];
    // let block_num_hexstr = &record["block_number"];

    let from_id = utils::addr_to_uuid(from.as_str());
    let v = Vertex::with_id(from_id, Identifier::new(from).unwrap());
    items.push(indradb::BulkInsertItem::Vertex(v));

    let to_id = utils::addr_to_uuid(to.as_str());
    let v = Vertex::with_id(to_id, Identifier::new(to).unwrap());
    items.push(indradb::BulkInsertItem::Vertex(v));

    let edge = Edge::new(from_id, Identifier::new(hash).unwrap(), to_id);
    items.push(indradb::BulkInsertItem::Edge(edge))

    // println!("pushed edge #{}", index); // 210_260_957 1_807_472_442
}
