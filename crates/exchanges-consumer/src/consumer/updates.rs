use super::{
    BlockMicroblockAppend, BlockchainUpdate, BlockchainUpdatesWithLastHeight, UpdatesSource,
};
use crate::{consumer::Tx, error::Error as AppError};
use anyhow::Result;
use async_trait::async_trait;
use bs58;
use chrono::Duration;
use std::{convert::TryFrom, str, time::Instant};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use waves_protobuf_schemas::waves::{
    events::{
        blockchain_updated::append::{BlockAppend, Body, MicroBlockAppend},
        blockchain_updated::Append,
        blockchain_updated::Update,
        grpc::{
            blockchain_updates_api_client::BlockchainUpdatesApiClient, SubscribeEvent,
            SubscribeRequest,
        },
        BlockchainUpdated,
    },
    SignedTransaction,
};

#[derive(Clone)]
pub struct UpdatesSourceImpl {
    grpc_client: BlockchainUpdatesApiClient<waves_protobuf_schemas::tonic::transport::Channel>,
}

pub async fn new(blockchain_updates_url: &str) -> Result<UpdatesSourceImpl> {
    Ok(UpdatesSourceImpl {
        grpc_client: BlockchainUpdatesApiClient::connect(blockchain_updates_url.to_owned()).await?,
    })
}

#[async_trait]
impl UpdatesSource for UpdatesSourceImpl {
    async fn stream(
        self,
        from_height: u32,
        batch_max_size: usize,
        batch_max_wait_time: Duration,
    ) -> Result<Receiver<BlockchainUpdatesWithLastHeight>> {
        let request = waves_protobuf_schemas::tonic::Request::new(SubscribeRequest {
            from_height: from_height as i32,
            to_height: 0,
        });

        let stream: waves_protobuf_schemas::tonic::Streaming<SubscribeEvent> = self
            .grpc_client
            .clone()
            .subscribe(request)
            .await?
            .into_inner();

        let (tx, rx) = channel::<BlockchainUpdatesWithLastHeight>(1);

        tokio::spawn(async move {
            self.run(stream, tx, from_height, batch_max_size, batch_max_wait_time)
                .await
        });

        Ok(rx)
    }
}

impl UpdatesSourceImpl {
    async fn run(
        &self,
        mut stream: waves_protobuf_schemas::tonic::Streaming<SubscribeEvent>,
        tx: Sender<BlockchainUpdatesWithLastHeight>,
        from_height: u32,
        batch_max_size: usize,
        batch_max_wait_time: Duration,
    ) -> Result<()> {
        let mut result = vec![];
        let mut last_height = from_height;

        let mut start = Instant::now();
        let mut should_receive_more = true;

        let batch_max_wait_time = batch_max_wait_time.to_std().unwrap();

        loop {
            if let Some(SubscribeEvent {
                update: Some(update),
            }) = stream.message().await?
            {
                last_height = update.height as u32;
                match BlockchainUpdate::try_from(update) {
                    Ok(upd) => Ok({
                        result.push(upd.clone());
                        match upd {
                            BlockchainUpdate::Block(_) => {
                                if result.len() >= batch_max_size
                                    || start.elapsed().ge(&batch_max_wait_time)
                                {
                                    should_receive_more = false;
                                }
                            }
                            BlockchainUpdate::Microblock(_) | BlockchainUpdate::Rollback(_) => {
                                should_receive_more = false
                            }
                        }
                    }),
                    Err(err) => Err(err),
                }?;
            }

            if !should_receive_more {
                tx.send(BlockchainUpdatesWithLastHeight {
                    last_height,
                    updates: result.clone(),
                })
                .await?;
                should_receive_more = true;
                start = Instant::now();
                result.clear();
            }
        }
    }
}

impl TryFrom<BlockchainUpdated> for BlockchainUpdate {
    type Error = AppError;

    fn try_from(value: BlockchainUpdated) -> Result<Self, Self::Error> {
        use BlockchainUpdate::{Block, Microblock, Rollback};

        match value.update {
            Some(Update::Append(Append {
                body,
                transaction_ids,
                transactions_metadata,
                transaction_state_updates,
                ..
            })) => {
                let height = value.height;

                let txs: Option<(Vec<SignedTransaction>, Option<i64>)> = match body {
                    Some(Body::Block(BlockAppend { ref block, .. })) => Ok(block
                        .clone()
                        .map(|it| (it.transactions, it.header.map(|it| it.timestamp)))),
                    Some(Body::MicroBlock(MicroBlockAppend {
                        ref micro_block, ..
                    })) => Ok(micro_block
                        .clone()
                        .and_then(|it| it.micro_block.map(|it| (it.transactions, None)))),
                    _ => Err(AppError::InvalidMessage(
                        "Append body is empty.".to_string(),
                    )),
                }?;

                let txs = match txs {
                    Some((txs, ..)) => txs
                        .into_iter()
                        .enumerate()
                        .filter_map(|(idx, tx)| {
                            let id = transaction_ids.get(idx).unwrap();
                            let meta = transactions_metadata.get(idx).unwrap();
                            let state_updates = transaction_state_updates.get(idx).unwrap();
                            Some(Tx {
                                id: bs58::encode(id).into_string(),
                                data: tx,
                                meta: meta.clone(),
                                state_updates: state_updates.clone(),
                            })
                        })
                        .collect(),
                    None => vec![],
                };

                match body {
                    Some(Body::Block(BlockAppend { block, .. })) => {
                        let time_stamp = block.and_then(|b| b.header.map(|h| h.timestamp));

                        Ok(Block(BlockMicroblockAppend {
                            id: bs58::encode(&value.id).into_string(),
                            time_stamp,
                            height: height as u32,
                            txs,
                        }))
                    }
                    Some(Body::MicroBlock(MicroBlockAppend { micro_block, .. })) => {
                        Ok(Microblock(BlockMicroblockAppend {
                            id: bs58::encode(&micro_block.as_ref().unwrap().total_block_id)
                                .into_string(),
                            time_stamp: None,
                            height: height as u32,
                            txs,
                        }))
                    }
                    _ => Err(AppError::InvalidMessage(
                        "Append body is empty.".to_string(),
                    )),
                }
            }
            Some(Update::Rollback(_)) => Ok(Rollback(bs58::encode(&value.id).into_string())),
            _ => Err(AppError::InvalidMessage(
                "Unknown blockchain update.".to_string(),
            )),
        }
    }
}
