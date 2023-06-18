use std::fs;

use clap::{arg, command, Parser};
use rocksdb::DB;

mod dump;
mod eth_common;
mod feature;
mod index;
mod link;
mod load;
mod repair;
mod subgraph;
mod unique;
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
    /// load the csv file into the graph database
    Load {
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
    /// load the subgraph from the graph database
    Subgraph {
        /// contains the verteies
        #[arg(short, long)]
        vertices: Vec<String>,

        /// or privide a file which contains the verteies
        #[arg(short, long)]
        input: Option<String>,

        /// max hop count
        #[arg(long, default_value_t = 1)]
        hop: usize,

        /// output filename
        #[arg(short, long, default_value = "subgraph.csv")]
        output: String,

        /// the graph file type of the output
        #[arg(value_enum, short, long, default_value_t = subgraph::GraphType::CsvEdgelist)]
        graph_type: subgraph::GraphType,

        /// the vertex type of the input
        #[arg(value_enum, long, default_value_t = subgraph::VType::ETHAddress)]
        v_type: subgraph::VType,

        /// the subgraph direction
        #[arg(value_enum, long, default_value_t = subgraph::Direction::Both)]
        direction: subgraph::Direction,

        /// carry props rather than txhash
        #[arg(long, value_delimiter = ',')]
        with_props: Vec<String>,
    },
    /// dump the graph database as json
    Dump {},
    /// repair the rocksdb
    Repair {},
    /// compact the rocksdb
    Compact {},
    /// extract vertex features
    Feature {
        /// contains the verteies
        #[arg(short, long)]
        vertices: Vec<String>,

        /// or privide a file which contains the verteies
        #[arg(short, long)]
        input: Option<String>,

        /// output filename
        #[arg(short, long, default_value = "features.csv")]
        feature_output: String,
    },
    /// link with a ethereum node
    Link {
        /// the ethereum endpoint url
        #[arg(short, long, default_value = "http://127.0.0.1:8545")]
        ethereum: String,

        /// thread count
        #[arg(short, long, default_value_t = 0)]
        thread_count: usize,

        /// the ending of the link sync
        #[arg(long, default_value_t = 0)]
        end: usize,
    },
    /// create an index on the property
    Index {
        /// field name
        #[arg(short, long)]
        name: String,
    },
    /// extract unique vertices/addresses from the file
    Edgelist {
        /// file name
        #[command(subcommand)]
        action: EdgelistAction,
    },
}

#[derive(clap::Subcommand, Debug)]
enum EdgelistAction {
    Unique { 
        #[arg(short, long)]
        filename: String 
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
    opts.set_target_file_size_base(0x4000000); // 64mb
    opts.set_level_zero_file_num_compaction_trigger(8);
    opts.set_level_zero_slowdown_writes_trigger(17);
    opts.set_level_zero_stop_writes_trigger(24);
    opts.set_num_levels(4);
    opts.set_max_bytes_for_level_base(536_870_912); // 512mb
    opts.set_max_bytes_for_level_multiplier(8.0);
    // opts.set_enable_blob_files(true);
    // opts.set_min_blob_size(0x0); // all in blob
    // opts.enable_statistics();

    // let v_count: usize = match datastore.get(AllVertexQuery.count().unwrap()).unwrap()[0] {
    //     QueryOutputValue::Count(count) => count.try_into().unwrap(),
    //     _ => todo!(),
    // };
    // log::warn!("all node: {:?}", v_count);

    match args.action {
        Action::Load { csv, fail, bulk } => {
            load::bulk_insert(args.rocks, &mut opts, csv, fail, bulk)
        }
        Action::Subgraph {
            mut vertices,
            input,
            hop,
            output,
            graph_type,
            v_type,
            direction,
            with_props: with_prop,
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
                v_type,
                direction,
                with_prop,
            )
        }
        Action::Dump {} => dump::json(args.rocks, &opts),
        Action::Repair {} => repair::repair_db(args.rocks, &opts),
        Action::Compact {} => {
            const CF_NAMES: [&str; 8] = [
                "vertices:v2",
                "edge_ranges:v2",
                "reversed_edge_ranges:v2",
                "vertex_properties:v2",
                "edge_properties:v2",
                "vertex_property_values:v2",
                "edge_property_values:v2",
                "metadata:v2",
            ];
            let db = DB::open_cf(&opts, args.rocks, CF_NAMES).unwrap();
            db.compact_range::<Vec<u8>, Vec<u8>>(None, None);
        }
        Action::Feature {
            mut vertices,
            input,
            feature_output,
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
                    let mut fe =
                        feature::FeatureExtracter::new(args.rocks, &mut opts, feature_output);
                    fe.gen_subgraph_features(&mut vertices).await
                })
        }
        Action::Link {
            ethereum,
            thread_count,
            end,
        } => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let thread_count = if thread_count <= 0 {
                    num_cpus::get()
                } else {
                    thread_count
                };

                let linker = link::Linker::new(ethereum, args.rocks, &mut opts).await;
                linker.sync(thread_count, end).await;
            }),
        Action::Index { name } => {
            index::create_index(args.rocks, &mut opts, name);
        }
        Action::Edgelist { action } => match action {
            EdgelistAction::Unique { filename } => unique::extract_unique_vertices(filename),
        },
        _ => panic!("unsupported"),
    }

    // drop(datastore);
}
