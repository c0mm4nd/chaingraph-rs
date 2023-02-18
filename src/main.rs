use clap::{arg, command, Parser};
use indradb::RocksdbDatastore;
mod dump;
mod load;
mod repair;
mod subgraph;
mod utils;

extern crate pretty_env_logger;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    action: Action,

    /// Rocksdb Path
    #[arg(short, long, default_value = "./rocks")]
    rocks: String,
}

#[derive(clap::Subcommand, Debug)]
enum Action {
    Insert {
        /// CSV File Path
        #[arg(short, long)]
        csv: String,

        /// Start line number
        #[arg(short, long, default_value_t = 0)]
        fail: usize,

        /// Bulk insert number
        #[arg(short, long, default_value_t = 10_000)]
        bulk: usize,
    },
    Subgraph {
        /// contains the verteies
        #[arg(short, long)]
        v: Vec<String>,

        /// Start line number
        #[arg(short, long, default_value_t = 1)]
        hop: usize,

        /// output filename
        #[arg(short, long, default_value = "subgraph.csv")]
        output: String,
    },
    Dump {},
    Repair {},
}

fn main() {
    pretty_env_logger::init_timed();
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

    // let v_count: usize = match datastore.get(AllVertexQuery.count().unwrap()).unwrap()[0] {
    //     QueryOutputValue::Count(count) => count.try_into().unwrap(),
    //     _ => todo!(),
    // };
    // log::warn!("all node: {:?}", v_count);

    match args.action {
        Action::Insert { csv, fail, bulk } => load::bulk_insert(
            RocksdbDatastore::new_db_with_options(args.rocks, &opts).unwrap(),
            csv,
            fail,
            bulk,
        ),
        Action::Subgraph { v, hop, output } => subgraph::gen(
            RocksdbDatastore::new_db_with_options(args.rocks, &opts).unwrap(),
            v,
            hop,
            output,
        ),
        Action::Dump {} => dump::json(args.rocks, &opts),
        Action::Repair {} => repair::repair_db(args.rocks, &opts),
    }

    // drop(datastore);
}
