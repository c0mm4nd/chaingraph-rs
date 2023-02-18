use indradb::{Database, RocksdbDatastore};
use rocksdb::Options;

pub fn repair_db(path: String, opts: &Options) {
    RocksdbDatastore::repair(path, opts).unwrap();
}
