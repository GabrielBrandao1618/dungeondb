use std::{
    io::{self, ErrorKind},
    sync::Arc,
};

use chest::{filter::bloom::BloomFilter, Chest};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream, ToSocketAddrs},
    signal,
    sync::Mutex,
    task::JoinHandle,
};

use runner::run_statement;
use server_value::{ServerError, ServerResponse};

pub struct Server {
    chest: Arc<Mutex<Chest>>,
    shutdown: Arc<Mutex<bool>>,
}

impl Server {
    pub fn new(chest: Chest) -> Self {
        Self {
            chest: Arc::new(Mutex::new(chest)),
            shutdown: Arc::new(Mutex::new(false)),
        }
    }
    pub async fn start<A: ToSocketAddrs>(&mut self, addr: A) -> io::Result<()> {
        let clone_shutdown = self.shutdown.clone();
        let shutdown_handle = tokio::spawn(async move {
            let _ = signal::ctrl_c().await;
            let mut shutdown_lock = clone_shutdown.lock().await;
            *shutdown_lock = true;
        });
        let socket = TcpListener::bind(addr).await?;
        let clone_chest = self.chest.clone();
        let clone_shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let _ = Self::listen(clone_chest, socket, clone_shutdown).await;
        });

        let _ = tokio::join!(shutdown_handle);

        Ok(())
    }
    async fn listen(
        chest: Arc<Mutex<Chest>>,
        socket: TcpListener,
        shutdown: Arc<Mutex<bool>>,
    ) -> io::Result<()> {
        loop {
            let shutdown_lock = shutdown.lock().await;
            if *shutdown_lock {
                return Ok(());
            }
            drop(shutdown_lock);
            let (stream, _) = socket.accept().await?;
            let chest = chest.clone();
            let _: JoinHandle<io::Result<()>> = tokio::spawn(async move {
                handle_connection(stream, chest).await?;
                Ok(())
            });
        }
    }
}

async fn handle_connection(mut stream: TcpStream, chest: Arc<Mutex<Chest>>) -> io::Result<()> {
    let (r, w) = stream.split();
    let mut r = BufReader::new(r);
    let mut w = BufWriter::new(w);
    loop {
        let mut input = String::new();
        let _ = r.read_line(&mut input).await?;
        if input.trim() == "exit" {
            let _ = drop(stream);
            break;
        }

        if !input.trim().is_empty() {
            let mut chest_lock = chest.lock().await;
            let result = run_statement(&mut *chest_lock, input.trim())
                .map(|v| ServerResponse::from_value(v))
                .unwrap_or_else(|err| ServerResponse::from_error(ServerError::new(&err.message)));
            let writable_result = result
                .to_vec()
                .map_err(|err| io::Error::new(ErrorKind::Other, err))?;
            w.write_all(&writable_result).await?;
            w.write("\n".as_bytes()).await?;
            w.flush().await?;
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
        let chest_clone = self.chest.clone();
        let replaced = std::mem::replace(&mut self.chest, chest_clone);
        drop(replaced);
    }
}
