use clap::{arg, command, Parser};
use crossbeam::deque::{Steal, Stealer, Worker};
use ethers::prelude::*;
use indradb::{
    AllEdgeQuery, AllVertexQuery, BulkInsertItem, CountQuery, CountQueryExt, Database, Datastore,
    Edge, Identifier, MemoryDatastore, QueryOutputValue, QueryOutputValue::Count, RocksdbDatastore,
    SpecificEdgeQuery, SpecificVertexQuery, Vertex,
};
use std::{
    borrow::Borrow,
    collections::HashMap,
    fs::File,
    io::{self, BufRead},
    str::FromStr,
    thread,
    time::Duration,
};
use uuid::Uuid;
extern crate pretty_env_logger;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// CSV File Path
    #[arg(short, long)]
    csv: String,

    /// Start line number
    #[arg(short, long, default_value_t = 0)]
    fail: usize,

    /// Rocksdb Path
    #[arg(short, long, default_value = "rocks")]
    rocks: String,

    /// Bulk insert number
    #[arg(short, long, default_value_t = 10_000)]
    bulk: usize,
}

type Record = HashMap<String, String>;

// #[tokio::main]
fn main() {
    pretty_env_logger::init();
    let args = Args::parse();

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.increase_parallelism(num_cpus::get().try_into().unwrap());
    opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
    opts.set_write_buffer_size(0x80000000); // 64mb
    opts.set_max_write_buffer_number(3);
    opts.set_target_file_size_base(67_108_864); // 64mb
    opts.set_level_zero_file_num_compaction_trigger(8);
    // opts.set_level_zero_slowdown_writes_trigger(17);
    opts.set_level_zero_slowdown_writes_trigger(-1);
    opts.set_level_zero_stop_writes_trigger(24);
    opts.set_num_levels(4);
    opts.set_max_bytes_for_level_base(536_870_912); // 512mb
    opts.set_max_bytes_for_level_multiplier(8.0);
    opts.enable_statistics();
    opts.set_disable_auto_compactions(true);

    let mut datastore = RocksdbDatastore::new_db_with_options(args.rocks, opts).unwrap();
    let v_count: usize = match datastore.get(AllVertexQuery.count().unwrap()).unwrap()[0] {
        QueryOutputValue::Count(count) => count.try_into().unwrap(),
        _ => todo!(),
    };
    println!("all node: {:?}", v_count);
    let e_count: usize = match datastore.get(AllEdgeQuery.count().unwrap()).unwrap()[0] {
        QueryOutputValue::Count(count) => count.try_into().unwrap(),
        _ => todo!(),
    };
    log::warn!("all edge: {:?}", e_count);

    let mut items = Vec::new();
    let mut reader = csv::Reader::from_path(args.csv).unwrap();

    let mut fail: usize = e_count;

    if args.fail != 0 {
        fail = args.fail
    }

    let trigger = args.bulk - 1;
    for (index, result) in reader.deserialize().enumerate() {
        if index < fail {
            continue;
        }

        let record: Record = result.unwrap();
        job(record, &mut items);
        if index % args.bulk == trigger {
            datastore.bulk_insert(items).unwrap();
            items = Vec::new();
            log::warn!("pushed edge #{} -> #{}", index - 999, index);

            // println!("{}", statistics.get_statistics().unwrap())
        }
    }
}

fn addr_to_uuid(addr: &str) -> Uuid {
    let address = Address::from_str(addr).unwrap();
    let id = Uuid::new_v5(&Uuid::NAMESPACE_OID, address.as_bytes());
    return id;
}

fn job(record: Record, items: &mut Vec<BulkInsertItem>) {
    let from = &record["from"];
    let to = &record["to"];
    let hash = &record["hash"];
    // let block_num_hexstr = &record["block_number"];

    let from_id = addr_to_uuid(from.as_str());
    let v = Vertex::with_id(from_id, Identifier::new(from).unwrap());
    items.push(indradb::BulkInsertItem::Vertex(v));

    let to_id = addr_to_uuid(to.as_str());
    let v = Vertex::with_id(to_id, Identifier::new(to).unwrap());
    items.push(indradb::BulkInsertItem::Vertex(v));

    let edge = Edge::new(from_id, Identifier::new(hash).unwrap(), to_id);
    items.push(indradb::BulkInsertItem::Edge(edge))

    // println!("pushed edge #{}", index); // 210_260_957 1_807_472_442
}
