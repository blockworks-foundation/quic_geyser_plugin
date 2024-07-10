use clap::Parser;
use quic_geyser_common::defaults::{
    DEFAULT_ACK_EXPONENT, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_MAX_ACK_DELAY,
    DEFAULT_MAX_RECIEVE_WINDOW_SIZE, DEFAULT_MAX_STREAMS,
};

#[derive(Parser, Debug, Clone)]
#[clap(name = "quic_plugin_tester")]
pub struct Args {
    #[clap(short, long)]
    pub source_url: String,

    #[clap(short, long, default_value_t = 10800)]
    pub port: u64,

    #[clap(short, long, default_value_t = 8)]
    pub compression_speed: i32,

    #[clap(short, long, default_value_t = 50)]
    pub max_number_of_connections: u64,

    #[clap(long, default_value_t = DEFAULT_MAX_STREAMS)]
    pub max_number_of_streams_per_client: u64,

    #[clap(long, default_value_t = DEFAULT_MAX_RECIEVE_WINDOW_SIZE)]
    pub recieve_window_size: u64,

    #[clap(long, default_value_t = DEFAULT_CONNECTION_TIMEOUT)]
    pub connection_timeout: u64,

    #[clap(long, default_value_t = DEFAULT_MAX_ACK_DELAY)]
    pub max_ack_delay: u64,

    #[clap(long, default_value_t = DEFAULT_ACK_EXPONENT)]
    pub ack_exponent: u64,
}
