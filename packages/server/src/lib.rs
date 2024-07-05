use std::{io, sync::Arc};

use chest::{filter::bloom::BloomFilter, Chest};
use grimoire::parse;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    sync::Mutex,
    task::JoinHandle,
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
        let socket = TcpListener::bind(addr).await?;
        let _ = self.listen(socket).await;
        Ok(())
    }
    async fn listen(&mut self, socket: TcpListener) -> io::Result<()> {
        while !self.shutdown {
            let (stream, _) = socket.accept().await?;
            let chest = self.chest.clone();
            let _: JoinHandle<io::Result<()>> = tokio::spawn(async move {
                handle_connection(stream, chest).await?;
                Ok(())
            });
        }
        Ok(())
    }
    pub async fn shutdown(&mut self) {
        self.shutdown = true;
    }
}

async fn handle_connection(mut stream: TcpStream, chest: Arc<Mutex<Chest>>) -> io::Result<()> {
    let (r, mut w) = stream.split();
    let mut r = BufReader::new(r);
    loop {
        let mut input = String::new();
        let _ = r.read_line(&mut input).await?;
        if input.trim() == "exit" {
            let _ = drop(stream);
            break;
        }

        if !input.trim().is_empty() {
            let parse_result = parse(input.trim());
            if let Ok(parsed) = parse_result {
                let mut chest_lock = chest.lock().await;
                let result = runner::run_query(&mut *chest_lock, parsed);
                if let Ok(result) = result {
                    w.write_all(format!("{}\n", result.to_string()).as_bytes())
                        .await?;
                    w.flush().await?;
                } else {
                    w.write_all(b"Error: Failed to run query").await?;
                }
            } else {
                w.write_all(b"Error: invalid statement\n").await?;
                w.flush().await?;
            }
        }
    }
    Ok(())
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
        let _ = self.shutdown();
    }
}
