use std::io;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
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
    pub fn disconnect(&mut self) {
        self.conn = None;
    }
    pub async fn query(&mut self, query: &str) -> io::Result<()> {
        if let Some(conn) = &mut self.conn {
            let (mut r, mut w) = conn.split();
            w.write_all(format!("{}\n", query).as_bytes()).await?;
            let mut input = String::new();
            let _ = r.read_to_string(&mut input).await?;
        }
        Ok(())
    }
}
