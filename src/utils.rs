use std::str::FromStr;

use bigdecimal::BigDecimal;
use ethers::types::{Address, U256};
use uuid::Uuid;

pub fn addr_to_uuid(addr: &str) -> Uuid {
    let address = Address::from_str(addr).unwrap();
    let id = Uuid::new_v5(&Uuid::NAMESPACE_OID, address.as_bytes()); // sha1 hash = 20bytes
    id
}

pub fn u256_to_bigdecimal(u256: U256) -> BigDecimal {
    BigDecimal::from_str(&u256.to_string()).unwrap()
}

pub fn gini(v: &Vec<f64>) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    // follows https://stackoverflow.com/questions/48999542/more-efficient-weighted-gini-coefficient-in-python
    let mut v = v.clone();
    v.sort_by(|a, b| a.total_cmp(b)); // https://users.rust-lang.org/t/how-to-sort-a-vec-of-floats/2838/3
    let n = v.len() as f64;
    let cumx = cumsum(v);
    let sum: f64 = cumx.iter().sum();
    ((n + 1.) - (2. * sum) / cumx[cumx.len() - 1]) / n
}

fn cumsum(v: Vec<f64>) -> Vec<f64> {
    v.into_iter()
        .scan(0.0, |acc, x| {
            *acc += x;
            Some(*acc)
        })
        .collect()
}

// pub fn u256_to_Float(u256: U256) -> BigDecimal {
//     let i = BigDecimal::from_str(&u256.to_string()).unwrap();
//     // Float::from(i)
//     i
// }

#[cfg(test)]
mod tests {
    use bigdecimal::ToPrimitive;

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_convert() {
        let a = u256_to_bigdecimal(U256::from_dec_str("15").unwrap());
        let b = u256_to_bigdecimal(U256::from_dec_str("10").unwrap());
        assert_eq!((a / b).to_f64().unwrap(), 1.5);
    }
}
