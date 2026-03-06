mod clickhouse;
mod errors;
mod handlers;
mod models;
mod utils;
mod validation;

use std::sync::Arc;

use actix_web::{App, HttpServer, web};

use clickhouse::ClickHouseClient;
use handlers::{
    health, healthz, metrics, v1_export_events_csv, v1_ingestion_lag, v1_pool_events,
    v1_pool_summary, v1_pools_top, v1_quality_latest, v1_quality_window, v1_swaps,
};
use models::{AppMetrics, AppState};
use utils::now_unix_ms;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();
    dotenv::from_filename(".env").ok();
    dotenv::from_filename("indexer/.env").ok();
    dotenv::from_filename("../.env").ok();
    dotenv::from_filename("../indexer/.env").ok();

    let host = std::env::var("API_HOST").unwrap();
    let port = std::env::var("API_PORT").unwrap().parse::<u16>().unwrap();
    let bind_addr = format!("{}:{}", host, port);

    let state = AppState {
        clickhouse: Arc::new(ClickHouseClient::from_env()),
        metrics: Arc::new(AppMetrics::default()),
        started_at_ms: now_unix_ms(),
    };

    println!("starting dune-project-api on {}", bind_addr);

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .service(health)
            .service(healthz)
            .service(metrics)
            .service(v1_swaps)
            .service(v1_pools_top)
            .service(v1_quality_latest)
            .service(v1_quality_window)
            .service(v1_ingestion_lag)
            .service(v1_pool_summary)
            .service(v1_pool_events)
            .service(v1_export_events_csv)
    })
    .shutdown_timeout(1)
    .bind(bind_addr)?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use crate::models::EventCursor;
    use crate::utils::{csv_escape, decode_cursor, encode_cursor, parse_event_values};

    #[test]
    fn cursor_round_trip() {
        let input = EventCursor {
            slot: 42,
            signature: "sig".to_string(),
            instruction_index: 7,
            inner_index: -1,
        };
        let encoded = encode_cursor(&input);
        let decoded = decode_cursor(&encoded).expect("decode cursor");
        assert_eq!(decoded.slot, input.slot);
        assert_eq!(decoded.signature, input.signature);
        assert_eq!(decoded.instruction_index, input.instruction_index);
        assert_eq!(decoded.inner_index, input.inner_index);
    }

    #[test]
    fn csv_escape_quotes_when_needed() {
        let escaped = csv_escape("hello,world");
        assert_eq!(escaped, "\"hello,world\"");
        let escaped_quotes = csv_escape("x\"y");
        assert_eq!(escaped_quotes, "\"x\"\"y\"");
    }

    #[test]
    fn parse_event_values_accepts_expected_chars() {
        let values = parse_event_values("swap,swap2,event_cpi::Swap,close-position")
            .expect("valid event list");
        assert_eq!(
            values,
            vec![
                "swap".to_string(),
                "swap2".to_string(),
                "event_cpi::Swap".to_string(),
                "close-position".to_string()
            ]
        );
    }

    #[test]
    fn parse_event_values_rejects_invalid_chars() {
        let err = parse_event_values("swap,drop table").expect_err("must reject spaces");
        assert_eq!(err, "drop table".to_string());
    }
}
