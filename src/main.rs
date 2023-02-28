use std::fs;

use clap::{arg, command, Parser};

mod dump;
mod feature;
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
        vertices: Vec<String>,

        /// or privide a file which contains the verteies
        #[arg(short, long)]
        input: Option<String>,

        /// Max hop count
        #[arg(long, default_value_t = 1)]
        hop: usize,

        /// output filename
        #[arg(short, long, default_value = "subgraph.csv")]
        output: String,

        /// output filename
        #[arg(value_enum, short, long, default_value_t = subgraph::GraphType::CsvAdj)]
        graph_type: subgraph::GraphType,
    },
    Dump {},
    Repair {},
    Feature {
        /// contains the verteies
        #[arg(short, long)]
        vertices: Vec<String>,

        /// or privide a file which contains the verteies
        #[arg(short, long)]
        input: Option<String>,

        /// Max hop count
        #[arg(long, default_value_t = 1)]
        hop: usize,

        /// output filename
        #[arg(short, long, default_value = "subgraph.csv")]
        output: String,
    },
}


fn main() {
    pretty_env_logger::init_timed();
    let args = Args::parse();

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.increase_parallelism(num_cpus::get().try_into().unwrap());
    opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);

    opts.set_max_write_buffer_number(3);
    opts.set_target_file_size_base(67_108_864); // 64mb
    opts.set_level_zero_file_num_compaction_trigger(8);
    // opts.set_level_zero_slowdown_writes_trigger(17);
    opts.set_level_zero_slowdown_writes_trigger(-1);
    opts.set_level_zero_stop_writes_trigger(24);
    opts.set_num_levels(4);
    opts.set_max_bytes_for_level_base(536_870_912); // 512mb
    opts.set_max_bytes_for_level_multiplier(8.0);
    opts.set_enable_blob_files(true);
    opts.enable_statistics();

    // let v_count: usize = match datastore.get(AllVertexQuery.count().unwrap()).unwrap()[0] {
    //     QueryOutputValue::Count(count) => count.try_into().unwrap(),
    //     _ => todo!(),
    // };
    // log::warn!("all node: {:?}", v_count);

    match args.action {
        Action::Insert { csv, fail, bulk } => {
            load::bulk_insert(args.rocks, &mut opts, csv, fail, bulk)
        }
        Action::Subgraph {
            mut vertices,
            input,
            hop,
            output,
            graph_type,
        } => {
            if let Some(input) = input {
                let content = fs::read_to_string(input).unwrap();
                vertices.extend(content.split_whitespace().map(|s| s.to_string()));
            }

            subgraph::gen_subgraph(
                args.rocks,
                &mut opts,
                &mut vertices,
                hop,
                output,
                graph_type,
            )
        }
        Action::Dump {} => dump::json(args.rocks, &opts),
        Action::Repair {} => repair::repair_db(args.rocks, &opts),
        Action::Feature {
            mut vertices,
            input,
            hop,
            output,
        } => {
            if let Some(input) = input {
                let content = fs::read_to_string(input).unwrap();
                vertices.extend(content.split_whitespace().map(|s| s.to_string()));
            }

            tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut fe = feature::FeatureExtracter::new(args.rocks, &mut opts);
                fe.gen_subgraph_features(
                    &mut vertices,
                    hop,
                    output,
                ).await
            })


        }
    }

    // drop(datastore);
}
