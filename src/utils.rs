use std::str::FromStr;

use ethers::types::Address;
use uuid::Uuid;

pub fn addr_to_uuid(addr: &str) -> Uuid {
    let address = Address::from_str(addr).unwrap();
    let id = Uuid::new_v5(&Uuid::NAMESPACE_OID, address.as_bytes()); // sha1 hash = 20bytes
    id
}

// pub fn uuid_to_addr(uuid: Uuid) -> String {
//     let bytes = uuid.as_bytes();

//     let address = Address::from_slice();
//     return address.to_string();
// }

// #[cfg(test)]
// mod tests {
//     // Note this useful idiom: importing names from outer (for mod tests) scope.
//     use super::*;

//     #[test]
//     fn test_convert() {
//         let addr = "0x05E793cE0C6027323Ac150F6d45C2344d28B6019".to_lowercase();
//         let uuid = addr_to_uuid(&addr);
//         let addr_2 = uuid_to_addr(uuid);
//         assert_eq!(addr, addr_2, "{} != {}", addr, addr_2);
//     }
// }
