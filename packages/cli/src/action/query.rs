use std::io;

use client::Client;

pub async fn query(url: String, query: String) -> io::Result<()> {
    let mut client = Client::new(url);
    client.connect().await?;
    let result = client.query(&query).await?;
    client.disconnect().await?;
    println!("{result}");
    Ok(())
}
