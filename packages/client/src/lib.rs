use std::{
    io::{self, BufWriter},
    time::Duration,
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
    time,
};

pub struct Client<A: ToSocketAddrs> {
    addrs: A,
    conn: Option<TcpStream>,
}

impl<A: ToSocketAddrs> Client<A> {
    pub fn new(addrs: A) -> Self {
        Self { addrs, conn: None }
    }
    pub async fn connect(&mut self) -> io::Result<()> {
        let connection = TcpStream::connect(&self.addrs).await?;
        self.conn = Some(connection);
        Ok(())
    }
    pub async fn disconnect(&mut self) -> io::Result<()> {
        if let Some(conn) = &mut self.conn {
            conn.shutdown().await?;
        }
        self.conn = None;
        Ok(())
    }
    pub async fn query(&mut self, query: &str) -> io::Result<String> {
        if let Some(conn) = &mut self.conn {
            conn.write_all(format!("{}\n", query).as_bytes()).await?;
            let mut input = Vec::new();
            conn.read(&mut input).await?;

            let parsed_input = String::from_utf8_lossy(&input).to_string();
            Ok(parsed_input)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Not connected to the server",
            ))
        }
    }
}

impl<A: ToSocketAddrs> Drop for Client<A> {
    fn drop(&mut self) {
        let _ = self.disconnect();
    }
}
