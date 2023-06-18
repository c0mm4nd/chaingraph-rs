use std::collections::{HashSet, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

use itertools::Itertools;

pub fn extract_unique_vertices(filename: String) {
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(file);

    let mut unique_addresses: BTreeSet<String> = BTreeSet::new();

    for line in reader.lines() {
        if let Ok(line) = line {
            let from_to_args: Vec<&str> = line.split_whitespace().collect();
            unique_addresses.insert(from_to_args[0].to_string());
            unique_addresses.insert(from_to_args[1].to_string());
        }
    }

    let mut output = File::create("unique.txt").unwrap();
    output.write_all(unique_addresses.iter().join("\n").as_bytes()).unwrap();
}
