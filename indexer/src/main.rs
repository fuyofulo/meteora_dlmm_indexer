#![allow(
    clippy::collapsible_else_if,
    clippy::collapsible_if,
    clippy::manual_is_multiple_of,
    clippy::question_mark,
    clippy::redundant_closure,
    clippy::too_many_arguments,
    clippy::wrong_self_convention
)]

use std::error::Error;

mod parser;
mod storage;
mod yellowstone;
use storage::BatchWriter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv::dotenv().ok();
    dotenv::from_filename("indexer/.env").ok();

    let yellowstone_endpoint = std::env::var("YELLOWSTONE_ENDPOINT").unwrap();
    let yellowstone_token = {
        let token = std::env::var("YELLOWSTONE_TOKEN").unwrap();
        let token = token.trim().to_string();
        if token.is_empty() { None } else { Some(token) }
    };
    let clickhouse_url = std::env::var("CLICKHOUSE_URL").unwrap();
    let clickhouse_database = std::env::var("CLICKHOUSE_DATABASE").unwrap();
    let batch_size = std::env::var("DB_BATCH_SIZE")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let batch_flush_ms = std::env::var("DB_BATCH_FLUSH_MS")
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let batch_queue_size = std::env::var("DB_BATCH_QUEUE_SIZE")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let db_queue_mode = std::env::var("DB_QUEUE_MODE").unwrap().to_ascii_lowercase();

    println!("Starting Yellowstone ingestor...");
    println!("   Endpoint: {}", yellowstone_endpoint);
    println!(
        "   Token: {}",
        if yellowstone_token.is_some() {
            "Set"
        } else {
            "Not set"
        }
    );
    println!("   ClickHouse URL: {}", clickhouse_url);
    println!("   ClickHouse database: {}", clickhouse_database);
    println!("   DB batch size: {}", batch_size);
    println!("   DB batch flush (ms): {}", batch_flush_ms);
    println!("   DB queue size: {}", batch_queue_size);
    println!("   DB queue mode: {}", db_queue_mode);

    let batch_writer = BatchWriter::new(
        &clickhouse_database,
        batch_size,
        batch_flush_ms,
        batch_queue_size,
    )?;

    let worker =
        yellowstone::YellowstoneWorker::new(yellowstone_endpoint, yellowstone_token, batch_writer);
    worker.run().await;

    Ok(())
}
