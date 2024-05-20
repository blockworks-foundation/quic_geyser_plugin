use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[clap(name = "quic_plugin_tester")]
pub struct Args {
    #[clap(short, long, default_value_t = 10900)]
    pub port: u32,

    #[clap(short, long, default_value_t = 20_000)]
    pub accounts_per_second: u32,

    #[clap(short = 'l', long, default_value_t = 1_000_000)]
    pub account_data_size: u32,

    #[clap(short, long, default_value_t = false)]
    pub drop_laggers: bool,
}
