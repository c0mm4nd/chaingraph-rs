use std::{fs::File, str::FromStr};

use crate::utils;
use async_recursion::async_recursion;
use bigdecimal::{BigDecimal, Zero, ToPrimitive};
use ethers::{prelude::*, providers::Provider, utils::WEI_IN_ETHER};
use hashbrown::HashSet;
use indradb::{
    Database, Edge, Identifier, QueryExt, QueryOutputValue, RocksdbDatastore, SpecificEdgeQuery,
    SpecificVertexQuery, Vertex,
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
        let count_in =  val_in_list.len() as f64;
        let count_out =  val_out_list.len() as f64;
        let count = count_in + count_out;

        let sum_val_in = val_in_list.iter().sum();
        let sum_val_out = val_out_list.iter().sum();

        let avg_val_in = if count_in.is_zero(){0.} else {sum_val_in as f64 / count_in};
        let avg_val_out = if count_out.is_zero() {0.} else {sum_val_out as f64/ count_out};

        let max_height_in = height_in_list.iter().max();
        let min_height_in = height_in_list.iter().min();
        let max_height_out = height_out_list.iter().max();
        let min_height_out = height_out_list.iter().min();
        let max_height = u64::max(*max_height_in.unwrap_or(&0), *max_height_out.unwrap_or(&0) );
        let min_height = u64::min(*min_height_in.unwrap_or(&16_200_000), *min_height_in.unwrap_or(&16_200_000) );

        let interval_in = match max_height_in {
            None => 0u64,
            Some(&max) => max - min_height_in.unwrap_or(&0), 
        };
        let interval_out = match max_height_in {
            None => 0u64,
            Some(&max) => max - min_height_out.unwrap_or(&0), 
        };
        let interval = if max_height <= min_height {0}else {max_height - min_height};

        let freq = if interval == 0 { f64::NAN } else { count / (interval as f64) };
        let freq_in = if interval_in == 0 {f64::NAN} else { count_in / (interval_in as f64) };
        let freq_out = if interval_out == 0 {f64::NAN} else { count_out / (interval_out as f64) };

        let val_list = [val_in_list.clone(), val_out_list.clone()].concat();

        let gini_val = utils::gini(&val_list);
        let gini_val_in = utils::gini(&val_in_list);
        let gini_val_out = utils::gini(&val_out_list);
        
        let sum_gas_in: f64 = gas_in_list.iter().sum();
        let sum_gas_out: f64 = gas_out_list.iter().sum();

        let avg_gas = (sum_gas_in + sum_gas_out) as f64 / (count_in + count_out);
        let avg_gas_in = if count_in.is_zero(){f64::NAN} else {sum_gas_in as f64 / count_in};
        let avg_gas_out = if count_out.is_zero() {f64::NAN} else {sum_gas_out as f64/ count_out};

        let sum_gasprice_in: f64 = gasprice_in_list.iter().sum();
        let sum_gasprice_out: f64 = gasprice_out_list.iter().sum();

        let avg_gasprice = (sum_gasprice_in + sum_gasprice_out) as f64 / (count_in + count_out);
        let avg_gasprice_in = if count_in.is_zero(){f64::NAN} else {sum_gasprice_in as f64 / count_in};
        let avg_gasprice_out = if count_out.is_zero() {f64::NAN} else {sum_gasprice_out as f64/ count_out};
 
        let in_out_rate = if count_out.is_zero() {0.} else { count_in / count_out };
        
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
            
            avg_gas,avg_gas_in,avg_gas_out, 
            
            avg_gasprice, avg_gasprice_in, avg_gasprice_out,

            in_out_rate, 
        }
    }
}

////////////////////////////////////////////////////////////////

pub struct FeatureExtracter {
    db: Database<RocksdbDatastore>,
    wei_in_eth: BigDecimal,

}

impl FeatureExtracter {
    pub fn new(
        path: String,
        opts: &mut Options,
    ) -> Self {
        opts.optimize_for_point_lookup(0x100000000);
        opts.set_optimize_filters_for_hits(true);
        opts.optimize_level_style_compaction(0x100000000);
        opts.set_memtable_whole_key_filtering(true);

        let db = RocksdbDatastore::new_db_with_options(path, opts).unwrap();
        FeatureExtracter { db, wei_in_eth: utils::u256_to_bigdecimal(WEI_IN_ETHER) }
    }

    pub async fn gen_subgraph_features(
        &mut self,
        v: &mut Vec<String>,
        hop: usize,
        output: String,
    ) {

        let provider = Provider::<Ws>::connect("ws://172.24.1.2:8545")
            .await
            .unwrap(); // // Provider::<Ws>::connect("wss://mainnet.infura.io/ws/v3/dc6980e1063b421bbcfef8d7f58ccd43")

        // convert v to ids
        v.sort();
        v.dedup();
        let ids: Vec<Uuid> = v.iter().map(|addr| utils::addr_to_uuid(addr)).collect();
        log::debug!("{} addresses", ids.len());

        let mut g_output = csv::Writer::from_path(output).unwrap();
        let mut f_output = csv::Writer::from_path("./features.out").unwrap();

        let q = SpecificVertexQuery::new(ids);
        let result = self.db.get(q).unwrap();

        for out_val in result {
            if let QueryOutputValue::Vertices(vertices) = out_val {
                let mut crawled_edges: HashSet<Identifier> = HashSet::new();
                let mut crawled_vertices: HashSet<Identifier> = HashSet::new();

                for v in &vertices {
                    self.run_hop(&provider, &mut g_output, &mut f_output, hop, v, &mut crawled_edges, &mut crawled_vertices)
                    .await
                }
            }
        }
    }

    #[async_recursion]
    async fn run_hop(
        &mut self,
        provider: &Provider<Ws>,
        g_output: &mut csv::Writer<File>,
        f_output: &mut csv::Writer<File>,
        hop: usize,
        v: &Vertex,
        crawled_edges: &mut HashSet<Identifier>,
        crawled_vertices: &mut HashSet<Identifier>,
    ) {
        if hop == 0 {
            return;
        };
        log::debug!("{:?}", v);
        let mut next_hop_vertices: Vec<Vertex> = Vec::new();

        let out_q = SpecificVertexQuery::single(v.id).outbound().unwrap();
        let out_e = self.db.get(out_q).unwrap();

        // init all lists
        let mut val_in_list = Vec::new(); 
        let mut val_out_list = Vec::new();
        let mut height_in_list= Vec::new();
        let mut height_out_list = Vec::new();
        let mut gas_in_list = Vec::new();
        let mut gas_out_list = Vec::new();
        let mut gasprice_in_list = Vec::new();
        let mut gasprice_out_list = Vec::new();

        for edges_list in out_e {
            let from = v.t.as_str();

            if let QueryOutputValue::Edges(edges) = edges_list {
                log::debug!("hop {}:  {} has {} outbound edges", hop, from, edges.len());

                for e in edges {
                    assert!(e.outbound_id == v.id, "{:?} != {:?}", e, v.id);

                    let tx_hash = H256::from_str(e.t.as_str()).unwrap();
                    let tx = provider.get_transaction(tx_hash).await.unwrap().unwrap();
                    val_out_list.push( (utils::u256_to_bigdecimal(tx.value) / &self.wei_in_eth).to_f64().unwrap() );
                    height_out_list.push( tx.block_number.unwrap().as_u64() );
                    gas_out_list.push( utils::u256_to_bigdecimal(tx.gas).to_f64().unwrap() );
                    gasprice_out_list.push( utils::u256_to_bigdecimal(tx.gas_price.unwrap_or(U256::from(0))).to_f64().unwrap() );

                    if crawled_edges.contains(&e.t) {
                        continue;
                    }
                    crawled_edges.insert(e.t);

                    let result = self.db
                        .get(SpecificVertexQuery::single(e.inbound_id))
                        .unwrap();
                    let result = &result[0]; // must be 1 len

                    if let QueryOutputValue::Vertices(tos) = result {
                        let to = &tos[0];
                        g_output.write_record([from, to.t.as_str(), &e.t]).unwrap();
                        next_hop_vertices.push(to.to_owned());
                    }
                }
            }
        }

        let in_q = SpecificVertexQuery::single(v.id).inbound().unwrap();
        let in_e = self.db.get(in_q).unwrap();

        for edges_list in in_e {
            let to = v.t.as_str();

            if let QueryOutputValue::Edges(edges) = edges_list {
                log::debug!("hop {}:  {} has {} inbound edges", hop, to, edges.len());

                for e in edges {
                    assert!(e.inbound_id == v.id);

                    let tx_hash = H256::from_str(e.t.as_str()).unwrap();
                    let tx = provider.get_transaction(tx_hash).await.unwrap().unwrap();
                    val_in_list.push( (utils::u256_to_bigdecimal(tx.value) / &self.wei_in_eth).to_f64().unwrap() );
                    height_in_list.push( tx.block_number.unwrap().as_u64() );
                    gas_in_list.push( utils::u256_to_bigdecimal(tx.gas).to_f64().unwrap() );
                    gasprice_in_list.push( utils::u256_to_bigdecimal(tx.gas_price.unwrap_or(U256::from(0))).to_f64().unwrap() );

                    if crawled_edges.contains(&e.t) {
                        continue;
                    }
                    crawled_edges.insert(e.t);

                    let result = self.db
                        .get(SpecificVertexQuery::single(e.outbound_id))
                        .unwrap();
                    let result = &result[0];

                    if let QueryOutputValue::Vertices(froms) = result {
                        let from = &froms[0]; // must only one
                        g_output.write_record([from.t.as_str(), to, &e.t]).unwrap();
                        next_hop_vertices.push(from.to_owned());
                    }
                }
            }
        }

        // write_feature start
        let addr = H160::from_str(v.t.as_str()).unwrap();
        let balance = provider.get_balance(addr, Some(16_200_000.into())).await.unwrap();
        let bal = (utils::u256_to_bigdecimal(balance) / &self.wei_in_eth).to_f64().unwrap();

        let addr_feature = AddressFeature::new(addr, bal, val_in_list, val_out_list, height_in_list, height_out_list, gas_in_list, gas_out_list, gasprice_in_list, gasprice_out_list);
        f_output.serialize(addr_feature).unwrap();
        // write feature end

        for next_v in next_hop_vertices {
            if crawled_vertices.contains(&next_v.t) {
                continue;
            }
            crawled_vertices.insert(next_v.t);
            self.run_hop(
                provider,
                g_output,
                f_output,
                hop - 1,
                &next_v,
                crawled_edges,
                crawled_vertices,
            ).await;
        }
    }
}
