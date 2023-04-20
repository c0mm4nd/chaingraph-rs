use indradb::{RocksdbDatastore, Identifier};
use rocksdb::Options;

pub fn create_index(
    path: String,
    opts: &mut Options,
    name: String,
) {
    let datastore = RocksdbDatastore::new_db_with_options(path, opts).unwrap();
    datastore.index_property(Identifier::new("details").unwrap());
}