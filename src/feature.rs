use std::{
    fs::File,
    str::FromStr,
    sync::{Arc, Mutex},
};

use crate::utils;
use bigdecimal::{BigDecimal, ToPrimitive, Zero};
use ethers::{prelude::*, providers::Provider, utils::WEI_IN_ETHER};
use indradb::{
    Database, QueryExt, QueryOutputValue, RocksdbDatastore, SpecificVertexQuery, Vertex,
};
use rocksdb::Options;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
struct AddressFeature {
    addr: H160,
    bal: f64,

    sum_val_in: f64,
    sum_val_out: f64,

    avg_val_in: f64,
    avg_val_out: f64,

    count: f64,
    count_in: f64,
    count_out: f64,

    freq: f64,
    freq_in: f64,
    freq_out: f64,

    gini_val: f64,
    gini_val_in: f64,
    gini_val_out: f64,

    avg_gas: f64,
    avg_gas_in: f64,
    avg_gas_out: f64,

    avg_gasprice: f64,
    avg_gasprice_in: f64,
    avg_gasprice_out: f64,

    in_out_rate: f64,
}

impl AddressFeature {
    fn new(
        addr: H160,
        bal: f64,
        val_in_list: Vec<f64>,
        val_out_list: Vec<f64>,
        height_in_list: Vec<u64>,
        height_out_list: Vec<u64>,
        gas_in_list: Vec<f64>,
        gas_out_list: Vec<f64>,
        gasprice_in_list: Vec<f64>,
        gasprice_out_list: Vec<f64>,
    ) -> Self {
        let count_in = val_in_list.len() as f64;
        let count_out = val_out_list.len() as f64;
        let count = count_in + count_out;

        let sum_val_in = val_in_list.iter().sum();
        let sum_val_out = val_out_list.iter().sum();

        let avg_val_in = if count_in.is_zero() {
            0.
        } else {
            sum_val_in / count_in
        };
        let avg_val_out = if count_out.is_zero() {
            0.
        } else {
            sum_val_out / count_out
        };

        let max_height_in = height_in_list.iter().max();
        let min_height_in = height_in_list.iter().min();
        let max_height_out = height_out_list.iter().max();
        let min_height_out = height_out_list.iter().min();
        let max_height = u64::max(*max_height_in.unwrap_or(&0), *max_height_out.unwrap_or(&0));
        let min_height = u64::min(
            *min_height_in.unwrap_or(&16_200_000),
            *min_height_in.unwrap_or(&16_200_000),
        );

        let interval_in = match max_height_in {
            None => 0u64,
            Some(&max) => max - min_height_in.unwrap_or(&0),
        };
        let interval_out = match max_height_in {
            None => 0u64,
            Some(&max) => max - min_height_out.unwrap_or(&0),
        };
        let interval = if max_height <= min_height {
            0
        } else {
            max_height - min_height
        };

        let freq = if interval == 0 {
            f64::NAN
        } else {
            count / (interval as f64)
        };
        let freq_in = if interval_in == 0 {
            f64::NAN
        } else {
            count_in / (interval_in as f64)
        };
        let freq_out = if interval_out == 0 {
            f64::NAN
        } else {
            count_out / (interval_out as f64)
        };

        let val_list = [val_in_list.clone(), val_out_list.clone()].concat();

        let gini_val = utils::gini(&val_list);
        let gini_val_in = utils::gini(&val_in_list);
        let gini_val_out = utils::gini(&val_out_list);

        let sum_gas_in: f64 = gas_in_list.iter().sum();
        let sum_gas_out: f64 = gas_out_list.iter().sum();

        let avg_gas = (sum_gas_in + sum_gas_out) / (count_in + count_out);
        let avg_gas_in = if count_in.is_zero() {
            f64::NAN
        } else {
            sum_gas_in / count_in
        };
        let avg_gas_out = if count_out.is_zero() {
            f64::NAN
        } else {
            sum_gas_out / count_out
        };

        let sum_gasprice_in: f64 = gasprice_in_list.iter().sum();
        let sum_gasprice_out: f64 = gasprice_out_list.iter().sum();

        let avg_gasprice = (sum_gasprice_in + sum_gasprice_out) / (count_in + count_out);
        let avg_gasprice_in = if count_in.is_zero() {
            f64::NAN
        } else {
            sum_gasprice_in / count_in
        };
        let avg_gasprice_out = if count_out.is_zero() {
            f64::NAN
        } else {
            sum_gasprice_out / count_out
        };

        let in_out_rate = if count_out.is_zero() {
            0.
        } else {
            count_in / count_out
        };

        AddressFeature {
            addr,
            bal,

            sum_val_in,
            sum_val_out,

            avg_val_in,
            avg_val_out,

            count,
            count_in,
            count_out,

            freq,
            freq_in,
            freq_out,

            gini_val,
            gini_val_in,
            gini_val_out,

            avg_gas,
            avg_gas_in,
            avg_gas_out,

            avg_gasprice,
            avg_gasprice_in,
            avg_gasprice_out,

            in_out_rate,
        }
    }
}

////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct FeatureExtracter {
    db: Arc<Database<RocksdbDatastore>>,
    wei_in_eth: BigDecimal,
    f_output: Arc<Mutex<csv::Writer<File>>>,
    max_sample: usize,
}

impl FeatureExtracter {
    pub fn new(path: String, opts: &mut Options, f_output: String, max_tx: usize) -> Self {
        opts.optimize_for_point_lookup(0x100000000);
        opts.set_optimize_filters_for_hits(true);
        opts.optimize_level_style_compaction(0x100000000);
        opts.set_memtable_whole_key_filtering(true);

        let db = RocksdbDatastore::new_db_with_options(path, opts).unwrap();

        let mut f_output = csv::Writer::from_path(f_output).unwrap();

        FeatureExtracter {
            db: Arc::new(db),

            wei_in_eth: utils::u256_to_bigdecimal(WEI_IN_ETHER),
            f_output: Arc::new(Mutex::new(f_output)),
            max_sample: max_tx,
        }
    }

    pub async fn gen_subgraph_features(&mut self, v: &mut Vec<String>) {
        let provider = Provider::<Ws>::connect("ws://127.0.0.1:8545")
            .await
            .unwrap(); // // Provider::<Ws>::connect("wss://mainnet.infura.io/ws/v3/dc6980e1063b421bbcfef8d7f58ccd43")

        // convert v to ids
        // v.sort();
        // v.dedup();
        let ids: Vec<Uuid> = v.iter().map(|addr| utils::addr_to_uuid(addr)).collect();
        log::debug!("{} addresses", ids.len());

        let q = SpecificVertexQuery::new(ids);
        let result = self.db.get(q).unwrap();
        let provider_arc = Arc::new(provider);

        let mut handles = Vec::new();
        assert_eq!(result.len(), 1);

        if let QueryOutputValue::Vertices(vertices) = result[0].clone() {
            for v in vertices {
                let db = Arc::clone(&self.db);
                let f_output = Arc::clone(&self.f_output);
                let provider = Arc::clone(&provider_arc);

                let max_sample = self.max_sample.clone();
                let wei_in_eth = self.wei_in_eth.clone();

                handles.push(tokio::spawn(async move {
                    Self::run_hop(db, provider, &v, f_output, max_sample, wei_in_eth)
                        .await;
                }));
            }
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    async fn run_hop(
        db: Arc<Database<RocksdbDatastore>>,
        provider: Arc<Provider<Ws>>,
        v: &Vertex,
        f_output: Arc<Mutex<csv::Writer<File>>>,
        max_sample: usize,
        wei_in_eth: BigDecimal,
    ) {
        log::debug!("{:?}", v);

        let out_q = SpecificVertexQuery::single(v.id).outbound().unwrap();
        let out_e = db.get(out_q).unwrap();

        // init all lists
        let mut val_in_list = Vec::with_capacity(max_sample);
        let mut val_out_list = Vec::with_capacity(max_sample);
        let mut height_in_list = Vec::with_capacity(max_sample);
        let mut height_out_list = Vec::with_capacity(max_sample);
        let mut gas_in_list = Vec::with_capacity(max_sample);
        let mut gas_out_list = Vec::with_capacity(max_sample);
        let mut gasprice_in_list = Vec::with_capacity(max_sample);
        let mut gasprice_out_list = Vec::with_capacity(max_sample);

        for edges_list in out_e {
            let from = v.t.as_str();

            if let QueryOutputValue::Edges(edges) = edges_list {
                log::debug!("{} has {} outbound edges", from, edges.len());

                for (i, e) in edges.iter().enumerate() {
                    assert!(e.outbound_id == v.id, "{:?} != {:?}", e, v.id);

                    if i < max_sample {
                        let tx_hash = H256::from_str(e.t.as_str()).unwrap();
                        let tx = provider.get_transaction(tx_hash).await.unwrap().unwrap();
                        val_out_list.push(
                            (utils::u256_to_bigdecimal(tx.value) / &wei_in_eth)
                                .to_f64()
                                .unwrap(),
                        );
                        height_out_list.push(tx.block_number.unwrap().as_u64());
                        gas_out_list.push(utils::u256_to_bigdecimal(tx.gas).to_f64().unwrap());
                        gasprice_out_list.push(
                            utils::u256_to_bigdecimal(tx.gas_price.unwrap_or(U256::from(0)))
                                .to_f64()
                                .unwrap(),
                        );
                    }
                }
            }
        }

        let in_q = SpecificVertexQuery::single(v.id).inbound().unwrap();
        let in_e = db.get(in_q).unwrap();

        for edges_list in in_e {
            let to = v.t.as_str();

            if let QueryOutputValue::Edges(edges) = edges_list {
                log::debug!("{} has {} inbound edges", to, edges.len());

                for (i, e) in edges.iter().enumerate() {
                    assert!(e.inbound_id == v.id);

                    if i < max_sample {
                        let tx_hash = H256::from_str(e.t.as_str()).unwrap();
                        let tx = provider.get_transaction(tx_hash).await.unwrap().unwrap();
                        val_in_list.push(
                            (utils::u256_to_bigdecimal(tx.value) / &wei_in_eth)
                                .to_f64()
                                .unwrap(),
                        );
                        height_in_list.push(tx.block_number.unwrap().as_u64());
                        gas_in_list.push(utils::u256_to_bigdecimal(tx.gas).to_f64().unwrap());
                        gasprice_in_list.push(
                            utils::u256_to_bigdecimal(tx.gas_price.unwrap_or(U256::from(0)))
                                .to_f64()
                                .unwrap(),
                        );
                    }
                }
            }
        }

        // write_feature start
        let addr = H160::from_str(v.t.as_str()).unwrap();
        let balance = provider
            .get_balance(addr, Some(16_200_000.into()))
            .await
            .unwrap();
        let bal = (utils::u256_to_bigdecimal(balance) / &wei_in_eth)
            .to_f64()
            .unwrap();

        let addr_feature = AddressFeature::new(
            addr,
            bal,
            val_in_list,
            val_out_list,
            height_in_list,
            height_out_list,
            gas_in_list,
            gas_out_list,
            gasprice_in_list,
            gasprice_out_list,
        );
        f_output.lock().unwrap().serialize(addr_feature).unwrap();
        // write feature end
    }
}
