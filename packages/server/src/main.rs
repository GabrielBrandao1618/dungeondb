use std::{
    env,
    io::{self},
};

use server::Server;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut args = env::args();
    let port = args.nth(1).unwrap_or("3000".to_owned());
    let mut server = Server::default();
    println!("Running server on port {port}");
    server.start(format!("localhost:{port}")).await?;
    Ok(())
}
