use std::{
    io::{self, ErrorKind},
    net::SocketAddr,
    sync::Arc,
};

use chest::Chest;
use grimoire::parse;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
};

pub struct Server {
    chest: Arc<Mutex<Chest>>,
}

impl Server {
    pub fn new(chest: Chest) -> Self {
        Self {
            chest: Arc::new(Mutex::new(chest)),
        }
    }
    pub async fn listen<A: Into<SocketAddr>>(&mut self, addr: A) -> io::Result<()> {
        let socket = TcpListener::bind(addr.into()).await?;
        loop {
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
    }
}
