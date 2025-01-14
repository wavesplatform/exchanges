pub mod storage;
pub mod updates;

use self::storage::BlockMicroblock;
use crate::consumer::convert::get_asset_id;
use crate::consumer::storage::ConsumerRepoOperations;
use crate::error::Error as AppError;
use anyhow::{Error, Result};
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use database::schema::exchange_transactions;
use itertools::Itertools;
use shared::waves::Address;
use std::{sync::Arc, time::Instant};
use tokio::sync::mpsc::Receiver;
use waves_protobuf_schemas::waves::events::transaction_metadata::{ExchangeMetadata, Metadata};
use waves_protobuf_schemas::waves::order::Side;
use waves_protobuf_schemas::waves::signed_transaction::Transaction::{
    EthereumTransaction, WavesTransaction,
};
use waves_protobuf_schemas::waves::{
    events::{StateUpdate, TransactionMetadata},
    transaction::Data,
    ExchangeTransactionData, SignedTransaction, Transaction,
};

#[allow(unused_imports)]
use wavesexchange_log::{debug, error, info, timer, trace, warn};

#[derive(Clone, Debug)]
pub enum BlockchainUpdate {
    Block(BlockMicroblockAppend),
    Microblock(BlockMicroblockAppend),
    Rollback(String),
}

#[derive(Clone, Debug)]
pub struct BlockMicroblockAppend {
    id: String,
    time_stamp: Option<i64>,
    height: u32,
    txs: Vec<Tx>,
}

#[derive(Debug)]
pub struct BlockchainUpdatesWithLastHeight {
    pub last_height: u32,
    pub updates: Vec<BlockchainUpdate>,
}

#[derive(Debug, Queryable)]
pub struct PrevHandledHeight {
    pub uid: i64,
    pub height: i32,
}

enum UpdatesItem {
    Blocks(Vec<BlockMicroblockAppend>),
    Microblock(BlockMicroblockAppend),
    Rollback(String),
}

#[derive(Clone, Debug)]
pub struct Tx {
    pub id: String,
    pub data: SignedTransaction,
    pub meta: TransactionMetadata,
    pub state_updates: StateUpdate,
}

#[derive(Clone, Debug)]
pub struct AnnotatedTx<'t> {
    pub block_uid: i64,
    pub height: u32,
    pub tx: &'t Tx,
}

#[async_trait::async_trait]
pub trait UpdatesSource {
    async fn stream(
        self,
        from_height: u32,
        batch_max_size: usize,
        batch_max_time: Duration,
    ) -> Result<Receiver<BlockchainUpdatesWithLastHeight>>;
}

#[derive(Clone, Debug, Insertable)]
#[diesel(table_name = exchange_transactions)]
pub struct InsertableExchangeTx {
    block_uid: i64,
    tx_date: NaiveDate,
    tx_id: String,
    /// This is the order sender, not transaction sender.
    sender: String,
    price_asset_id: String,
    price: i64,
    amount_asset_id: String,
    amount: i64,
    order_amount: i64,
    fee_asset_id: Option<String>,
    fee: Option<i64>,
    /// +1 = Buy, -1 = Sell
    buy_sell: i32,
}

pub async fn start<T, R>(
    starting_height: u32,
    updates_src: T,
    storage: &mut R,
    updates_per_request: usize,
    max_wait_time_in_secs: u64,
    matcher_address: Arc<String>,
) -> Result<()>
where
    T: UpdatesSource + Send + Sync + 'static,
    R: storage::ConsumerRepo,
{
    let starting_from_height = match storage.execute(|ops| ops.get_first_height_in_last_day())? {
        Some(prev_handled_height) => {
            storage.transaction(|ops| rollback(ops, prev_handled_height.uid))?;
            prev_handled_height.height as u32 + 1
        }
        None => starting_height,
    };

    info!(
        "Start fetching updates from height {}",
        starting_from_height
    );
    let max_duration = Duration::seconds(max_wait_time_in_secs as i64);

    let mut rx = updates_src
        .stream(starting_from_height, updates_per_request, max_duration)
        .await?;

    loop {
        let mut start = Instant::now();

        let updates_with_height = rx.recv().await.ok_or(Error::new(AppError::StreamClosed(
            "GRPC Stream was closed by the server".to_string(),
        )))?;

        let updates_count = updates_with_height.updates.len();
        info!(
            "{} updates were received in {:?}",
            updates_count,
            start.elapsed()
        );

        let last_height = updates_with_height.last_height;

        let matcher_address = matcher_address.clone();

        start = Instant::now();

        storage.transaction(|ops| {
            handle_updates(updates_with_height, ops, matcher_address)?;

            info!(
                "{} updates were handled in {:?} ms. Last updated height is {}.",
                updates_count,
                start.elapsed().as_millis(),
                last_height
            );

            Ok(())
        })?;
    }
}

fn handle_updates<R: ConsumerRepoOperations>(
    updates_with_height: BlockchainUpdatesWithLastHeight,
    storage: &mut R,
    matcher_address: Arc<String>,
) -> Result<()> {
    updates_with_height
        .updates
        .into_iter()
        .fold::<&mut Vec<UpdatesItem>, _>(&mut vec![], |acc, cur| match cur {
            BlockchainUpdate::Block(b) => {
                info!("Handle block {}, height = {}", b.id, b.height);
                let len = acc.len();
                if len > 0 {
                    match acc.iter_mut().nth(len - 1).unwrap() {
                        UpdatesItem::Blocks(v) => {
                            v.push(b);
                            acc
                        }
                        UpdatesItem::Microblock(_) | UpdatesItem::Rollback(_) => {
                            acc.push(UpdatesItem::Blocks(vec![b]));
                            acc
                        }
                    }
                } else {
                    acc.push(UpdatesItem::Blocks(vec![b]));
                    acc
                }
            }
            BlockchainUpdate::Microblock(mba) => {
                info!("Handle microblock {}, height = {}", mba.id, mba.height);
                acc.push(UpdatesItem::Microblock(mba));
                acc
            }
            BlockchainUpdate::Rollback(sig) => {
                info!("Handle rollback to {}", sig);
                acc.push(UpdatesItem::Rollback(sig));
                acc
            }
        })
        .into_iter()
        .try_fold((), |_, update_item| match update_item {
            UpdatesItem::Blocks(bs) => {
                squash_microblocks(storage)?;
                handle_appends(storage, bs.as_ref(), false, matcher_address.clone())
            }
            UpdatesItem::Microblock(mba) => handle_appends(
                storage,
                &vec![mba.to_owned()],
                true,
                matcher_address.clone(),
            ),
            UpdatesItem::Rollback(sig) => {
                let block_uid = storage.get_block_uid(&sig)?;
                rollback(storage, block_uid)
            }
        })?;

    Ok(())
}

fn handle_appends<R: ConsumerRepoOperations>(
    storage: &mut R,
    appends: &Vec<BlockMicroblockAppend>,
    is_microblock: bool,
    matcher_address: Arc<String>,
) -> Result<()> {
    let block_uids = storage.insert_blocks_or_microblocks(
        &appends
            .into_iter()
            .map(|append| BlockMicroblock {
                id: append.id.clone(),
                height: append.height as i32,
                time_stamp: append.time_stamp,
            })
            .collect_vec(),
    )?;

    let annotated_txs = block_uids
        .into_iter()
        .zip(appends)
        .filter(|&(_, append)| append.txs.len() > 0)
        .flat_map(|(block_uid, append)| {
            append.txs.iter().map(move |tx| AnnotatedTx {
                block_uid,
                height: append.height,
                tx,
            })
        });

    let txs_with_block_uids = annotated_txs
        .to_owned()
        .flat_map(|ann_tx| extract_exchange_txs(&ann_tx, &matcher_address))
        .collect_vec();

    storage.insert_exchange_transactions(&txs_with_block_uids)?;

    if !is_microblock {
        storage.update_aggregates()?;
    }

    info!("extracted and handled {} ", txs_with_block_uids.len());

    Ok(())
}

/// Converts each Exchange Transaction from our matcher
/// into a pair of `InsertableExchangeTx` structs (one per initial order).
/// So this fn always returns either 0 or 2 items.
fn extract_exchange_txs(ann_tx: &AnnotatedTx, matcher_address: &str) -> Vec<InsertableExchangeTx> {
    match ann_tx.tx.data.transaction.as_ref() {
        None => vec![],
        Some(EthereumTransaction(_)) => vec![], // Ethereum transactions are ignored
        Some(WavesTransaction(Transaction {
            data, timestamp, ..
        })) => {
            match data.as_ref() {
                Some(Data::Exchange(ExchangeTransactionData {
                    orders,
                    amount,
                    price,
                    buy_matcher_fee,
                    sell_matcher_fee,
                    ..
                })) => {
                    let tx_sender = Address::new(&ann_tx.tx.meta.sender_address);
                    if *tx_sender.into_string() != *matcher_address {
                        trace!("ExchangeTx not from our matcher: {:?}", ann_tx.tx.id);
                        return vec![]; // Skip transactions from other matchers
                    }

                    let time_stamp = {
                        DateTime::<Utc>::from_naive_utc_and_offset(
                            NaiveDateTime::from_timestamp_opt(
                                *timestamp / 1000,
                                *timestamp as u32 % 1000 * 1000,
                            )
                            .expect("invalid or out-of-range datetime"),
                            Utc,
                        )
                    };

                    // ExchangeTransaction have only 2 orders
                    orders.into_iter().enumerate().map(|(i, order)| {
                        let sender_address = match &ann_tx.tx.meta.metadata {
                            Some(Metadata::Exchange(ExchangeMetadata {
                                order_sender_addresses,
                                ..
                            })) => Address::new(&order_sender_addresses[i]).into_string(),
                            _ => panic!("tx_id:{} can't get sender address from Metadata::Exchange(ExchangeMetadata {{order_sender_addresses[{}]}})", ann_tx.tx.id, i),
                        };

                        let asset_pair =
                            order.asset_pair.as_ref().expect("order.asset_pair is None");

                        let fee_asset_id = match order.matcher_fee.as_ref() {
                            Some(f) => Some(get_asset_id(&f.asset_id)),
                            _ => None,
                        };

                        let fee = match order.order_side() {
                            Side::Buy => Some(*buy_matcher_fee),
                            Side::Sell => Some(*sell_matcher_fee),
                        };

                        let buy_sell = match order.order_side() {
                            Side::Buy => 1,
                            Side::Sell => -1,
                        };

                        let amount_asset_id = get_asset_id(&asset_pair.amount_asset_id);
                        let price_asset_id = get_asset_id(&asset_pair.price_asset_id);

                        InsertableExchangeTx {
                            block_uid: ann_tx.block_uid,
                            tx_date: time_stamp.date_naive(),
                            tx_id: ann_tx.tx.id.clone(),
                            sender: sender_address,
                            price_asset_id,
                            price: *price,
                            amount_asset_id,
                            amount: *amount,
                            order_amount: order.amount,
                            fee_asset_id,
                            fee,
                            buy_sell,
                        }
                    }).collect()
                }
                _ => vec![],
            }
        }
    }
}

fn squash_microblocks<R: ConsumerRepoOperations>(storage: &mut R) -> Result<()> {
    let total_block_id = storage.get_total_block_id()?;

    match total_block_id {
        Some(total_block_id) => {
            let key_block_uid = storage.get_key_block_uid()?;

            storage.update_exchange_transactions_block_references(&key_block_uid)?;

            storage.delete_microblocks()?;

            storage.change_block_id(&key_block_uid, &total_block_id)?;
        }
        None => (),
    }

    Ok(())
}

fn rollback<R: ConsumerRepoOperations>(storage: &mut R, block_uid: i64) -> Result<()> {
    debug!("rolling back to block_uid = {}", block_uid);

    storage.rollback_blocks_microblocks(&block_uid)
}

mod convert {
    pub(super) fn get_asset_id<I: AsRef<[u8]>>(input: I) -> String {
        if input.as_ref().is_empty() {
            "WAVES".to_owned()
        } else {
            bs58::encode(input).into_string()
        }
    }
}
