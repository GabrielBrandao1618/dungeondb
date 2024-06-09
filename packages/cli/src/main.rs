use clap::{command, Parser, Subcommand};

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

fn main() {
    let args = Args::parse();
    match args.cmd {
        Action::Query { url, query } => {
            println!("Running query: `{query}` on server {url}");
        }
        Action::Connect { url } => println!("Connecting to server at {url}"),
    }
}
