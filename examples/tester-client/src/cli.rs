use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[clap(name = "quic_plugin_tester")]
pub struct Args {
    #[clap(short, long)]
    pub url: String,

    #[clap(short, long)]
    pub rpc_url: Option<String>,

    #[clap(short, long, default_value_t = false)]
    pub blocks_instead_of_accounts: bool,

    #[clap(short = 's', long, default_value_t = 1_000)]
    pub number_of_streams: u64,
}
