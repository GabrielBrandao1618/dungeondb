use std::{
    io::{self, ErrorKind},
    sync::Arc,
};

use chest::{filter::bloom::BloomFilter, Chest};
use grimoire::parse;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, ToSocketAddrs},
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
        let socket = TcpListener::bind(addr).await?;
        let _ = self.listen(socket).await;
        Ok(())
    }
    async fn listen(&mut self, socket: TcpListener) -> io::Result<()> {
        while !self.shutdown {
            let (stream, _) = socket.accept().await?;
            let chest = self.chest.clone();
            tokio::spawn(async move {
                handle_connection(stream, chest).await.unwrap();
            });
        }
        Ok(())
    }
    pub async fn shutdown(&mut self) {
        self.shutdown = true;
    }
}

async fn handle_connection(mut stream: TcpStream, chest: Arc<Mutex<Chest>>) -> io::Result<()> {
    let (mut r, mut w) = stream.split();
    loop {
        let mut input: Vec<u8> = Vec::new();
        let chars_read = r.read_buf(&mut input).await?;
        if chars_read > 0 {
            let parsed_input = String::from_utf8_lossy(&input);
            if parsed_input.trim() == "exit" {
                let _ = drop(stream);
                break;
            }

            let parsed = parse(parsed_input.trim())
                .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
            let mut chest_lock = chest.lock().await;
            let result = runner::run_query(&mut *chest_lock, parsed)
                .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
            w.write_all(format!("{}\n", result.to_string()).as_bytes())
                .await?;
            w.flush().await?;
            input.clear();
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
