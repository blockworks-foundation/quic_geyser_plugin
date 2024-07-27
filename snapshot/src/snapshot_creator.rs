use std::{collections::HashMap, str::FromStr, sync::Arc};

use itertools::Itertools;
use lite_account_manager_common::{
    account_data::{Account as AccountManagerAccount, AccountData as AccountManagerAccountData},
    account_filter::{AccountFilter, AccountFilterType},
    account_filters_interface::AccountFiltersStoreInterface,
    account_store_interface::{AccountLoadingError, AccountStorageInterface},
    commitment::Commitment,
    except_filter_store::ExceptFilterStore,
    slot_info::SlotInfo,
};
use lite_account_storage::{
    inmemory_account_store::InmemoryAccountStore, storage_by_program_id::StorageByProgramId,
};
use lite_token_account_storage::{
    inmemory_token_account_storage::InmemoryTokenAccountStorage,
    inmemory_token_storage::TokenProgramAccountsStorage, TOKEN_PROGRAM_2022_ID, TOKEN_PROGRAM_ID,
};
use quic_geyser_common::{channel_message::ChannelMessage, config::CompressionParameters};
use solana_sdk::pubkey::Pubkey;

use crate::snapshot_config::SnapshotConfig;

pub struct SnapshotCreator {
    storage: Arc<dyn AccountStorageInterface>,
    filters: Arc<dyn AccountFiltersStoreInterface>,
    compression_mode: CompressionParameters,
}

impl SnapshotCreator {
    pub fn new(config: SnapshotConfig, compression_mode: CompressionParameters) -> SnapshotCreator {
        let program_ids_except = config
            .load_program_ids_except
            .iter()
            .map(|pk| Pubkey::from_str(pk).unwrap())
            .collect_vec();
        let mut program_storage_map = HashMap::new();
        let token_account_storage = Arc::new(InmemoryTokenAccountStorage::default());
        let token_program_storage: Arc<dyn AccountStorageInterface> =
            Arc::new(TokenProgramAccountsStorage::new(token_account_storage));
        program_storage_map.insert(TOKEN_PROGRAM_2022_ID, token_program_storage.clone());
        program_storage_map.insert(TOKEN_PROGRAM_ID, token_program_storage);
        let mut filters = ExceptFilterStore::default();

        let account_filters = program_ids_except
            .iter()
            .map(|f| AccountFilter {
                accounts: vec![],
                program_id: Some(*f),
                filters: None,
            })
            .collect_vec();
        filters.add_account_filters(&account_filters);
        let filters = Arc::new(filters);
        let default = Arc::new(InmemoryAccountStore::new(filters.clone()));

        SnapshotCreator {
            storage: Arc::new(StorageByProgramId::new(program_storage_map, default)),
            filters,
            compression_mode,
        }
    }

    pub fn start_listening(
        &self,
        mut recieve_channel: tokio::sync::mpsc::UnboundedReceiver<ChannelMessage>,
    ) {
        let storage = self.storage.clone();
        let filters = self.filters.clone();
        let compression = self.compression_mode.clone();
        tokio::spawn(async move {
            while let Some(message) = recieve_channel.recv().await {
                match message {
                    ChannelMessage::Account(account, slot, is_init) => {
                        let tmp_acc = AccountManagerAccountData {
                            pubkey: account.pubkey,
                            account: Arc::new(AccountManagerAccount {
                                lamports: account.account.lamports,
                                data: lite_account_manager_common::account_data::Data::Uncompressed(
                                    vec![],
                                ),
                                owner: account.account.owner,
                                executable: account.account.executable,
                                rent_epoch: account.account.rent_epoch,
                            }),
                            updated_slot: slot,
                            write_version: 0,
                        };
                        // check first if filter is satified
                        if !filters.satisfies(&tmp_acc) {
                            continue;
                        }

                        let is_token_program = account.account.owner == TOKEN_PROGRAM_2022_ID
                            || account.account.owner == TOKEN_PROGRAM_ID;
                        let data = account.account.data;
                        let data_len = data.len();
                        let may_be_compressed_data = if is_token_program {
                            lite_account_manager_common::account_data::Data::Uncompressed(data)
                        } else {
                            match compression.compression_type {
                                quic_geyser_common::compression::CompressionType::None => {
                                    lite_account_manager_common::account_data::Data::Uncompressed(
                                        data,
                                    )
                                }
                                quic_geyser_common::compression::CompressionType::Lz4Fast(_)
                                | quic_geyser_common::compression::CompressionType::Lz4(_) => {
                                    lite_account_manager_common::account_data::Data::Lz4 {
                                        binary: compression.compression_type.compress(&data),
                                        len: data_len,
                                    }
                                }
                            }
                        };

                        let account_to_save = AccountManagerAccountData {
                            pubkey: account.pubkey,
                            account: Arc::new(AccountManagerAccount {
                                lamports: account.account.lamports,
                                data: may_be_compressed_data,
                                owner: account.account.owner,
                                executable: account.account.executable,
                                rent_epoch: account.account.rent_epoch,
                            }),
                            updated_slot: slot,
                            write_version: 0,
                        };
                        if is_init {
                            storage.initilize_or_update_account(account_to_save)
                        } else {
                            storage.update_account(
                                account_to_save,
                                lite_account_manager_common::commitment::Commitment::Processed,
                            );
                        }
                    }
                    ChannelMessage::Slot(slot, parent, commitment) => {
                        storage.process_slot_data(
                            SlotInfo {
                                slot,
                                parent,
                                root: 0,
                            },
                            commitment.into(),
                        );
                    }
                    _ => {
                        // other message are not treated
                    }
                }
            }
            log::error!("snapshot creator listening thread stopped");
            panic!();
        });
    }

    pub fn create_snapshot(&self, program_id: Pubkey) -> Result<Vec<u8>, AccountLoadingError> {
        self.storage.create_snapshot(program_id)
    }

    pub fn get_program_accounts(
        &self,
        program_pubkey: Pubkey,
        account_filters: Option<Vec<AccountFilterType>>,
        commitment: Commitment,
    ) -> Result<Vec<AccountManagerAccountData>, AccountLoadingError> {
        self.storage
            .get_program_accounts(program_pubkey, account_filters, commitment)
    }
}
