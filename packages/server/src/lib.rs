use std::{
    io::{self, ErrorKind},
    sync::Arc,
};

use chest::{filter::bloom::BloomFilter, Chest};
use grimoire::parse;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, ToSocketAddrs},
    sync::Mutex,
};

pub struct Server {
    chest: Arc<Mutex<Chest>>,
    shutdown: bool,
}

impl Server {
    pub fn new(chest: Chest) -> Self {
        Self {
            chest: Arc::new(Mutex::new(chest)),
            shutdown: false,
        }
    }
    pub async fn start<A: ToSocketAddrs>(&mut self, addr: A) -> io::Result<()> {
        println!("Started");

        let socket = TcpListener::bind(addr).await?;
        let _ = self.listen(socket).await;
        Ok(())
    }
    async fn listen(&mut self, socket: TcpListener) -> io::Result<()> {
        while !self.shutdown {
            let (mut stream, _) = socket.accept().await?;
            let chest = self.chest.clone();
            let handle: io::Result<()> = tokio::spawn(async move {
                let (mut r, mut w) = stream.split();
                loop {
                    let mut input: Vec<u8> = Vec::new();
                    let _ = r.read_buf(&mut input).await?;
                    let parsed = parse(&String::from_utf8_lossy(&input))
                        .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
                    let mut chest_lock = chest.lock().await;
                    let result = runner::run_query(&mut *chest_lock, parsed)
                        .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
                    w.write(format!("{}\n", result.to_string()).as_bytes())
                        .await?;
                }
            })
            .await?;
            if let Err(err) = handle {
                eprintln!("{}", err.to_string());
            }
        }
        Ok(())
    }
    pub async fn shutdown(&mut self) {
        self.shutdown = true;
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new(
            Chest::new(".chest", 512, 24, Box::new(BloomFilter::new(1024, 1.0)))
                .expect("Could not create chest"),
        )
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        println!("Dropping");
        let _ = self.shutdown();
    }
}
