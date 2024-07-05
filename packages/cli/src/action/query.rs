use std::io;

use client::Client;

pub async fn query(url: String, query: String) -> io::Result<()> {
    let mut client = Client::new(url);
    client.connect().await?;
    let result = client.query(&query).await?;
    client.query("exit").await?;

    client.disconnect().await?;
    print!("{result}");
    Ok(())
}
