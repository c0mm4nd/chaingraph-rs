# ChainGraph

Build and Query the blockchain **transaction** network.

chaingraph-rs is written in rust, aiming to do fast query on the giant transaction network

## Build

```bash
git clone https://github.com/c0mm4nd/chaingraph-rs
cd chaingraph-rs
cargo build --release
```

## Usage

load all transaction

```bash
Usage: chaingraph-rs [OPTIONS] <COMMAND>

Commands:
  load      load the csv file into the graph database
  subgraph  load the subgraph from the graph database
  dump      dump the graph database as json
  repair    repair the rocksdb
  compact   compact the rocksdb
  feature   extract vertex features
  link      link with a ethereum node
  index     create an index on the property
  help      Print this message or the help of the given subcommand(s)

Options:
  -r, --rocks <ROCKS>  Rocksdb Path [default: ./rocks]
  -h, --help           Print help
  -V, --version        Print version
```

### load

```bash
load the csv file into the graph database

Usage: chaingraph-rs load [OPTIONS] --csv <CSV>

Options:
  -c, --csv <CSV>    CSV File Path
  -f, --fail <FAIL>  Start line number [default: 0]
  -b, --bulk <BULK>  Bulk insert number [default: 10000]
  -h, --help         Print help
```

### subgraph

```bash
load the subgraph from the graph database

Usage: chaingraph-rs subgraph [OPTIONS]

Options:
  -v, --vertices <VERTICES>      contains the verteies
  -i, --input <INPUT>            or privide a file which contains the verteies
      --hop <HOP>                max hop count [default: 1]
  -o, --output <OUTPUT>          output filename [default: subgraph.csv]
  -g, --graph-type <GRAPH_TYPE>  the graph file type of the output [default: csv-edgelist] [possible values: csv-edgelist, csv-nodes, rdf]
      --v-type <V_TYPE>          the vertex type of the input [default: eth-address] [possible values: eth-address, string]
      --direction <DIRECTION>    the subgraph direction [default: both] [possible values: out, in, both]
      --with-props <WITH_PROPS>  carry props rather than txhash
  -h, --help                     Print help
```

### feature

```bash
extract vertex features

Usage: chaingraph-rs feature [OPTIONS]

Options:
  -v, --vertices <VERTICES>              contains the verteies
  -i, --input <INPUT>                    or privide a file which contains the verteies
  -f, --feature-output <FEATURE_OUTPUT>  output filename [default: features.csv]
  -h, --help                             Print help
```

## FAQ

`Error { message: "IO error: While open a file for random read: ../eth_graph_16800000_fix_create/007558.sst: Too many open files" }`

suggest using `root`, try `ulimit -n 65535` or daemon
