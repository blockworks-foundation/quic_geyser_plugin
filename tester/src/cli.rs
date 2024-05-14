use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long)]
    pub url: String,

    #[arg(short, long)]
    pub rpc_url: String,
}
