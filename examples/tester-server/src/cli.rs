use clap::Parser;
use quic_geyser_common::defaults::DEFAULT_MAX_STREAMS;

#[derive(Parser, Debug, Clone)]
#[clap(name = "quic_plugin_tester")]
pub struct Args {
    #[clap(short, long, default_value_t = 10900)]
    pub port: u32,

    #[clap(short, long, default_value_t = 20_000)]
    pub accounts_per_second: u32,

    #[clap(short = 'm', long, default_value_t = 200)]
    pub min_account_data_size: usize,

    #[clap(short = 'x', long, default_value_t = 5000)]
    pub max_account_data_size: usize,

    #[clap(short = 'l', long, default_value_t = 1000)]
    pub number_of_random_accounts: usize,

    #[clap(short = 's', long, default_value_t = DEFAULT_MAX_STREAMS)]
    pub number_of_streams: u64,
}
