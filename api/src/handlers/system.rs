use actix_web::{HttpResponse, Responder, get, web};
use serde_json::json;

use crate::models::{AppState, HealthResponse};
use crate::utils::{now_unix_ms, parse_u64_or_zero};
use crate::validation::first_row_or_empty;

async fn health_payload(state: web::Data<AppState>) -> HttpResponse {
    state.metrics.inc_request();
    let clickhouse_ok = match state
        .clickhouse
        .query_scalar_u8("SELECT toUInt8(1) AS value")
    {
        Ok(v) => v == 1,
        Err(_) => false,
    };

    HttpResponse::Ok().json(HealthResponse {
        status: "ok",
        service: "dune-project-api",
        clickhouse_ok,
    })
}

#[get("/health")]
pub async fn health(state: web::Data<AppState>) -> impl Responder {
    health_payload(state).await
}

#[get("/healthz")]
pub async fn healthz(state: web::Data<AppState>) -> impl Responder {
    health_payload(state).await
}

#[get("/metrics")]
pub async fn metrics(state: web::Data<AppState>) -> impl Responder {
    state.metrics.inc_request();
    let uptime_seconds = now_unix_ms()
        .saturating_sub(state.started_at_ms)
        .saturating_div(1000);

    let body = format!(
        concat!(
            "# TYPE dune_api_requests_total counter\n",
            "dune_api_requests_total {}\n",
            "# TYPE dune_api_requests_failed_total counter\n",
            "dune_api_requests_failed_total {}\n",
            "# TYPE dune_api_clickhouse_errors_total counter\n",
            "dune_api_clickhouse_errors_total {}\n",
            "# TYPE dune_api_bad_requests_total counter\n",
            "dune_api_bad_requests_total {}\n",
            "# TYPE dune_api_uptime_seconds gauge\n",
            "dune_api_uptime_seconds {}\n"
        ),
        state.metrics.requests_total(),
        state.metrics.requests_failed(),
        state.metrics.clickhouse_errors(),
        state.metrics.bad_requests(),
        uptime_seconds
    );

    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/plain; version=0.0.4"))
        .body(body)
}

#[get("/v1/ingestion/lag")]
pub async fn v1_ingestion_lag(state: web::Data<AppState>) -> impl Responder {
    state.metrics.inc_request();
    let bronze_table = state.clickhouse.table_ref("bronze_raw_updates");
    let silver_table = state.clickhouse.table_ref("silver_dlmm_events");
    let quality_table = state.clickhouse.table_ref("gold_quality_minute");
    let sql = format!(
        "SELECT
            (SELECT coalesce(max(ingested_at_ms), 0) FROM {bronze_table}) AS bronze_ingested_at_ms,
            (SELECT coalesce(max(ingested_at_ms), 0) FROM {silver_table}) AS silver_ingested_at_ms,
            (SELECT coalesce(max(last_ingested_unix_ms), 0) FROM {quality_table}) AS quality_ingested_at_ms,
            (SELECT coalesce(max(last_slot), 0) FROM {quality_table}) AS last_slot"
    );

    let row = match first_row_or_empty(&state, &sql) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let now_ms = now_unix_ms();
    let bronze_ms = parse_u64_or_zero(row.get("bronze_ingested_at_ms"));
    let silver_ms = parse_u64_or_zero(row.get("silver_ingested_at_ms"));
    let quality_ms = parse_u64_or_zero(row.get("quality_ingested_at_ms"));

    HttpResponse::Ok().json(json!({
        "now_unix_ms": now_ms,
        "last_slot": parse_u64_or_zero(row.get("last_slot")),
        "bronze_ingested_at_ms": bronze_ms,
        "silver_ingested_at_ms": silver_ms,
        "quality_ingested_at_ms": quality_ms,
        "bronze_lag_ms": now_ms.saturating_sub(bronze_ms),
        "silver_lag_ms": now_ms.saturating_sub(silver_ms),
        "quality_lag_ms": now_ms.saturating_sub(quality_ms)
    }))
}
