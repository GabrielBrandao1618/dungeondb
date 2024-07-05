use std::io;

use action::{connect::connect, query};
use clap::{command, Parser, Subcommand};

mod action;

#[derive(Parser, Debug)]
struct Args {
    #[command(subcommand)]
    cmd: Action,
}

#[derive(Subcommand, Debug, Clone)]
enum Action {
    #[command(about = "Run a single query in a given server address")]
    Query { url: String, query: String },
    #[command(about = "Connect to a given server address")]
    Connect { url: String },
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();
    match args.cmd {
        Action::Query {
            url,
            query: query_arg,
        } => {
            query::query(url, query_arg).await?;
        }
        Action::Connect { url } => connect(url).await?,
    }
    Ok(())
}
