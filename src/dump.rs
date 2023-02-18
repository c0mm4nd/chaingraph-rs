use rocksdb::{IteratorMode, Options, DB};

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

pub fn json(path: String, opts: &Options) {
    let db = DB::open_cf(opts, path, CF_NAMES).unwrap();
    for cf_name in CF_NAMES {
        let iter = db.iterator_cf(db.cf_handle(cf_name).unwrap(), IteratorMode::Start);
        for row in iter {
            let (k, v) = row.unwrap();
            println!("{cf_name}: {k:?} -> {v:?}");
        }
    }
}
