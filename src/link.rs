use ethers::{
    providers::{Middleware, Provider, ProviderExt, Ws},
    types::{serde_helpers, transaction::eip2930::AccessList},
    types::{Address, Bytes, Log, H256, U256, U64},
};
use indradb::{
    BulkInsertItem, Database, Edge, Identifier, RocksdbDatastore, SpecificVertexQuery, Vertex,
};
use serde::{Deserialize, Serialize};
use std::{rc::Rc, sync::Arc};
use tokio::sync::mpsc::channel;
use uuid::Uuid;

use crate::{utils::{self, addr_to_uuid}, eth_common::TransactionInfo};

#[derive(Clone)]
pub struct Linker {
    db: Arc<Database<RocksdbDatastore>>,
    provider: Provider<Ws>,
}

impl Linker {
    pub async fn new(ethereum: String, path: String, opts: &mut rocksdb::Options) -> Self {
        opts.set_disable_auto_compactions(true);
        opts.set_write_buffer_size(0x80000000); // 2G
        // opts.set_enable_blob_files(true);
        // opts.set_enable_blob_gc(false);
        // opts.set_blob_compression_type(rocksdb::DBCompressionType::None);
        opts.prepare_for_bulk_load();

        let provider = Provider::<Ws>::connect(ethereum).await.unwrap();

        let db = RocksdbDatastore::new_db_with_options(path, opts).unwrap();
        return Linker {
            db: Arc::new(db),
            provider,
        };
    }

    pub async fn handle_fork() {}

    pub fn update_block_in_db(self) {
        let id = Uuid::new_v5(&Uuid::NAMESPACE_OID, &[]);
        let q = SpecificVertexQuery::single(id);
        self.db.get(q).unwrap();
    }

    pub fn get_latest_from_db(self) {
        let id = Uuid::new_v5(&Uuid::NAMESPACE_OID, &[]);
        let q = SpecificVertexQuery::single(id);
        self.db.get(q).unwrap();
    }

    pub async fn job(self, thread_id: usize, thread_count: usize, end: usize) {
        // start jobs
        let mut num = thread_id;
        loop {
            if num > end {
                break;
            }

            let receipts_promise = self.provider.get_block_receipts(num);
            let block_promise = self.provider.get_block_with_txs(num as u64);

            let (receipts, block) = tokio::join!(receipts_promise, block_promise);
            let receipts = receipts.unwrap();

            match block.unwrap() {
                None => {
                    todo!()
                }
                Some(block) => {
                    let mut items: Vec<BulkInsertItem> = Vec::new();
                    for (index, tx) in block.transactions.iter().enumerate() {
                        match tx.to {
                            None => {
                                let from_id = utils::h160_to_uuid(&tx.from);
                                let v = Vertex::with_id(
                                    from_id,
                                    Identifier::new(ethers::utils::to_checksum(&tx.from, None))
                                        .unwrap(),
                                );
                                items.push(indradb::BulkInsertItem::Vertex(v));

                                let to = &receipts[index].contract_address.unwrap();

                                let to_id = utils::h160_to_uuid(&to);
                                let v = Vertex::with_id(
                                    to_id,
                                    Identifier::new(ethers::utils::to_checksum(&to, None)).unwrap(),
                                );
                                items.push(indradb::BulkInsertItem::Vertex(v));

                                let edge = Edge::new(
                                    from_id,
                                    Identifier::new(ethers::utils::hex::encode(&tx.hash)).unwrap(),
                                    to_id,
                                );
                                items.push(indradb::BulkInsertItem::Edge(edge.clone()));

                                let receipt = &receipts[index];
                                let info = TransactionInfo {
                                    hash: tx.hash,
                                    nonce: tx.nonce,
                                    block_hash: tx.block_hash,
                                    block_number: tx.block_number,
                                    transaction_index: tx.transaction_index,
                                    from: tx.from,
                                    to: tx.to,
                                    value: tx.value,
                                    gas_price: tx.gas_price,
                                    gas: tx.gas,
                                    input: tx.input.clone(),
                                    v: tx.v,
                                    r: tx.r,
                                    s: tx.s,
                                    transaction_type: tx.transaction_type,
                                    max_fee_per_gas: tx.max_fee_per_gas,
                                    max_priority_fee_per_gas: tx.max_priority_fee_per_gas,
                                    cumulative_gas_used: receipt.cumulative_gas_used,
                                    gas_used: receipt.gas_used,
                                    is_create: !(receipt.contract_address.is_none()),
                                    // logs: receipt.logs
                                    status: receipt.status,
                                    effective_gas_price: receipt.effective_gas_price,
                                };

                                // add props
                                items.push(indradb::BulkInsertItem::EdgeProperty(
                                    edge.clone(),
                                    Identifier::new("details").unwrap(),
                                    indradb::Json::new(serde_json::to_value(info).unwrap()),
                                ));
                            }
                            Some(to) => {
                                let from_id = utils::h160_to_uuid(&tx.from);
                                let v = Vertex::with_id(
                                    from_id,
                                    Identifier::new(ethers::utils::to_checksum(&tx.from, None))
                                        .unwrap(),
                                );
                                items.push(indradb::BulkInsertItem::Vertex(v));

                                let to_id = utils::h160_to_uuid(&to);
                                let v = Vertex::with_id(
                                    to_id,
                                    Identifier::new(ethers::utils::to_checksum(&to, None)).unwrap(),
                                );
                                items.push(indradb::BulkInsertItem::Vertex(v));

                                let edge = Edge::new(
                                    from_id,
                                    Identifier::new(ethers::utils::hex::encode(&tx.hash)).unwrap(),
                                    to_id,
                                );
                                items.push(indradb::BulkInsertItem::Edge(edge.clone()));

                                let receipt = &receipts[index];
                                let info = TransactionInfo {
                                    hash: tx.hash,
                                    nonce: tx.nonce,
                                    block_hash: tx.block_hash,
                                    block_number: tx.block_number,
                                    transaction_index: tx.transaction_index,
                                    from: tx.from,
                                    to: tx.to,
                                    value: tx.value,
                                    gas_price: tx.gas_price,
                                    gas: tx.gas,
                                    input: tx.input.clone(),
                                    v: tx.v,
                                    r: tx.r,
                                    s: tx.s,
                                    transaction_type: tx.transaction_type,
                                    max_fee_per_gas: tx.max_fee_per_gas,
                                    max_priority_fee_per_gas: tx.max_priority_fee_per_gas,
                                    cumulative_gas_used: receipt.cumulative_gas_used,
                                    gas_used: receipt.gas_used,
                                    is_create: !(receipt.contract_address.is_none()),
                                    // logs: receipt.logs
                                    status: receipt.status,
                                    effective_gas_price: receipt.effective_gas_price,
                                };

                                // add props
                                items.push(indradb::BulkInsertItem::EdgeProperty(
                                    edge.clone(),
                                    Identifier::new("details").unwrap(),
                                    indradb::Json::new(serde_json::to_value(info).unwrap()),
                                ));
                            }
                        }
                    }
                    self.db.bulk_insert(items).unwrap();
                    log::debug!("{} done", num);
                }
            }

            num += thread_count;
        }
    }

    pub async fn sync(self, thread_count: usize, end: usize) {
        // try get latest from indra
        // get_latest_from_db()

        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            // Your handler here
        });

        let mut handlers = vec![];

        for id in 0..thread_count {
            let s = self.clone();
            handlers.push(tokio::spawn(s.job(id, thread_count, end)));
        }

        for handler in handlers {
            handler.await.unwrap()
        }

        //
    }
}
