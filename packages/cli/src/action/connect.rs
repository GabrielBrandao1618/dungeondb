use client::Client;
use std::io;
use tokio::io::{stdin, AsyncBufReadExt, BufReader};

pub async fn connect(url: String) -> io::Result<()> {
    let mut client = Client::new(url);
    client.connect().await?;

    let mut r = BufReader::new(stdin());

    loop {
        let mut input = String::new();
        let _ = r.read_line(&mut input).await?;

        let result = client.query(&input).await?;
        print!("{result}");
        if input.trim() == "exit" {
            break;
        }
    }

    Ok(())
}
