use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
};
use quic_geyser_common::types::block_meta::BlockMeta;
use solana_sdk::{
    account::Account, clock::Slot, commitment_config::CommitmentLevel, pubkey::Pubkey,
    signature::Keypair,
};

use crate::{
    config::Config,
    plugin_error::QuicGeyserError,
    quic::quic_server::{AccountData, ChannelMessage, QuicServer},
};

#[derive(Debug, Default)]
pub struct QuicGeyserPlugin {
    quic_server: Option<QuicServer>,
}

impl GeyserPlugin for QuicGeyserPlugin {
    fn name(&self) -> &'static str {
        "quic_geyser_plugin"
    }

    fn on_load(&mut self, config_file: &str) -> PluginResult<()> {
        log::info!("loading quic_geyser plugin");
        let config = Config::load_from_file(config_file)?;
        log::info!("Quic plugin config correctly loaded");

        let quic_server = QuicServer::new(Keypair::new(), config).map_err(|_| {
            GeyserPluginError::Custom(Box::new(QuicGeyserError::ErrorConfiguringServer))
        })?;
        self.quic_server = Some(quic_server);

        Ok(())
    }

    fn on_unload(&mut self) {
        self.quic_server = None;
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: Slot,
        is_startup: bool,
    ) -> PluginResult<()> {
        let Some(quic_server) = &self.quic_server else {
            return Ok(());
        };
        let ReplicaAccountInfoVersions::V0_0_3(account_info) = account else {
            return Err(GeyserPluginError::AccountsUpdateError {
                msg: "Unsupported account info version".to_string(),
            });
        };
        let account = Account {
            lamports: account_info.lamports,
            data: account_info.data.to_vec(),
            owner: Pubkey::try_from(account_info.owner).expect("valid pubkey"),
            executable: account_info.executable,
            rent_epoch: account_info.rent_epoch,
        };
        let pubkey: Pubkey = Pubkey::try_from(account_info.pubkey).expect("valid pubkey");
        quic_server.send_message(ChannelMessage::Account(
            AccountData {
                pubkey,
                account,
                write_version: account_info.write_version,
            },
            slot,
            is_startup,
        ))?;
        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: Slot,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        // Todo
        let Some(quic_server) = &self.quic_server else {
            return Ok(());
        };
        let commitment_level = match status {
            SlotStatus::Processed => CommitmentLevel::Processed,
            SlotStatus::Rooted => CommitmentLevel::Finalized,
            SlotStatus::Confirmed => CommitmentLevel::Confirmed,
        };
        let slot_message = ChannelMessage::Slot(slot, parent.unwrap_or_default(), commitment_level);
        quic_server.send_message(slot_message)?;
        Ok(())
    }

    fn notify_transaction(
        &self,
        _transaction: ReplicaTransactionInfoVersions,
        _slot: Slot,
    ) -> PluginResult<()> {
        // Todo
        Ok(())
    }

    fn notify_entry(&self, _entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        // Not required
        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> PluginResult<()> {
        let Some(quic_server) = &self.quic_server else {
            return Ok(());
        };

        let ReplicaBlockInfoVersions::V0_0_3(blockinfo) = blockinfo else {
            return Err(GeyserPluginError::AccountsUpdateError {
                msg: "Unsupported account info version".to_string(),
            });
        };

        let block_meta = BlockMeta {
            parent_slot: blockinfo.parent_slot,
            slot: blockinfo.slot,
            parent_blockhash: blockinfo.parent_blockhash.to_string(),
            blockhash: blockinfo.blockhash.to_string(),
            rewards: blockinfo.rewards.to_vec(),
            block_height: blockinfo.block_height,
            executed_transaction_count: blockinfo.executed_transaction_count,
            entries_count: blockinfo.entry_count,
        };

        quic_server.send_message(ChannelMessage::BlockMeta(block_meta))?;
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        false
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = QuicGeyserPlugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
