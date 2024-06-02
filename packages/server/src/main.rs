use std::{
    io::{self, ErrorKind},
    sync::Arc,
};

use chest::{filter::bloom::BloomFilter, Chest};
use grimoire::parse;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};

#[tokio::main]
async fn main() -> io::Result<()> {
    let chest = Arc::new(Mutex::new(
        Chest::new(".chest", 64, 128, Box::new(BloomFilter::new(1024, 1.0)))
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?,
    ));
    let socket = TcpListener::bind("127.0.0.1:3000").await?;
    loop {
        let (mut stream, _) = socket.accept().await?;
        let chest = chest.clone();
        let handle: io::Result<()> = tokio::spawn(async move {
            let (mut r, mut w) = stream.split();
            let mut input: Vec<u8> = Vec::new();
            let _ = r.read_buf(&mut input).await?;
            let parsed = parse(&String::from_utf8_lossy(&input))
                .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
            let mut chest_lock = chest.lock().await;
            let result = runner::run_query(&mut *chest_lock, parsed)
                .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
            w.write(format!("{}\n", result.to_string()).as_bytes())
                .await?;
            Ok(())
        })
        .await?;
        if let Err(err) = handle {
            eprintln!("{}", err.to_string());
        }
    }
}
