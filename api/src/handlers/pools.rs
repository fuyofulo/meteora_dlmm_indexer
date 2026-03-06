use actix_web::{HttpResponse, Responder, get, web};
use serde_json::json;

use crate::errors::bad_request;
use crate::models::{
    AppState, PaginatedResponse, PoolEventItem, PoolEventsQuery, PoolSummaryQuery, TopPoolItem,
    TopPoolsQuery,
};
use crate::utils::{
    now_unix_ms, parse_bool, parse_i64_or, parse_string, parse_string_or, parse_string_or_empty,
    parse_u64, parse_u64_or_zero, sql_quote,
};
use crate::validation::{
    first_row_or_empty, parse_event_filter, query_rows_or_500, validate_slot_range,
    validated_limit, validated_minutes,
};

use super::{
    append_cursor_predicate, append_event_in_predicate, decode_optional_cursor, encode_next_cursor,
};

#[get("/v1/pools/top")]
pub async fn v1_pools_top(
    query: web::Query<TopPoolsQuery>,
    state: web::Data<AppState>,
) -> impl Responder {
    state.metrics.inc_request();
    let minutes = match validated_minutes(&state, "minutes", query.minutes, 60, 1, 10080) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let limit = match validated_limit(&state, "limit", query.limit, 20, 1, 200) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let gold_table = state.clickhouse.table_ref("gold_pool_minute");
    let sql = format!(
        "SELECT
            pool,
            sum(swap_count) AS swap_count,
            toString(sum(volume_raw)) AS volume_raw,
            sum(unique_users) AS unique_users_sum,
            max(last_ingested_unix_ms) AS last_ingested_unix_ms
        FROM {gold_table}
        WHERE minute_bucket >= toInt64(intDiv(toUnixTimestamp(now()), 60) - {minutes})
        GROUP BY pool
        ORDER BY toUInt128(volume_raw) DESC, swap_count DESC
        LIMIT {limit}"
    );

    let rows = match query_rows_or_500(&state, &sql) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let items = rows
        .into_iter()
        .map(|row| TopPoolItem {
            pool: parse_string_or_empty(row.get("pool")),
            swap_count: parse_u64_or_zero(row.get("swap_count")),
            volume_raw: parse_string_or(row.get("volume_raw"), "0"),
            unique_users_sum: parse_u64_or_zero(row.get("unique_users_sum")),
            last_ingested_unix_ms: parse_u64_or_zero(row.get("last_ingested_unix_ms")),
        })
        .collect::<Vec<_>>();

    HttpResponse::Ok().json(json!({
        "minutes": minutes,
        "limit": limit,
        "items": items
    }))
}

#[get("/v1/pools/{pool}/summary")]
pub async fn v1_pool_summary(
    path: web::Path<String>,
    query: web::Query<PoolSummaryQuery>,
    state: web::Data<AppState>,
) -> impl Responder {
    state.metrics.inc_request();
    let pool = path.into_inner();
    if pool.trim().is_empty() {
        return bad_request(&state, "invalid_pool", "`pool` cannot be empty", None);
    }
    let minutes = match validated_minutes(&state, "minutes", query.minutes, 60, 1, 10080) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let hours = (minutes as u64).div_ceil(60);
    let now_ms = now_unix_ms();
    let cutoff_ms = now_ms.saturating_sub((minutes as u64) * 60_000);

    let pool_minute_table = state.clickhouse.table_ref("gold_pool_minute");
    let pool_user_hour_table = state.clickhouse.table_ref("gold_pool_user_hour");
    let silver_table = state.clickhouse.table_ref("silver_dlmm_events");

    let minute_sql = format!(
        "SELECT
            sum(swap_count) AS swap_count,
            toString(sum(volume_raw)) AS volume_raw,
            sum(unique_users) AS unique_users_sum,
            min(min_slot) AS min_slot,
            max(max_slot) AS max_slot,
            max(last_ingested_unix_ms) AS last_ingested_unix_ms
        FROM {pool_minute_table}
        WHERE pool = {pool}
          AND minute_bucket >= toInt64(intDiv(toUnixTimestamp(now()), 60) - {minutes})",
        pool = sql_quote(&pool),
    );

    let hour_sql = format!(
        "SELECT
            countDistinct(user) AS active_users,
            sum(claim_events) AS claim_events,
            toString(sum(fee_x_raw)) AS fee_x_raw,
            toString(sum(fee_y_raw)) AS fee_y_raw
        FROM {pool_user_hour_table}
        WHERE pool = {pool}
          AND hour_bucket >= toInt64(intDiv(toUnixTimestamp(now()), 3600) - {hours})",
        pool = sql_quote(&pool),
    );

    let events_sql = format!(
        "SELECT
            count() AS events_total,
            countIf(parsed = 1) AS parsed_events,
            countIf(notEmpty(ifNull(parse_error, ''))) AS parse_error_events,
            countIf(notEmpty(ifNull(parse_warning, ''))) AS parse_warning_events
        FROM {silver_table}
        WHERE ifNull(pool, '') = {pool}
          AND ingested_at_ms >= {cutoff_ms}",
        pool = sql_quote(&pool),
    );

    let minute_row = match first_row_or_empty(&state, &minute_sql) {
        Ok(row) => row,
        Err(resp) => return resp,
    };

    let hour_row = match first_row_or_empty(&state, &hour_sql) {
        Ok(row) => row,
        Err(resp) => return resp,
    };

    let events_row = match first_row_or_empty(&state, &events_sql) {
        Ok(row) => row,
        Err(resp) => return resp,
    };

    HttpResponse::Ok().json(json!({
        "pool": pool,
        "minutes": minutes,
        "window": {
            "from_ingested_at_ms": cutoff_ms,
            "to_ingested_at_ms": now_ms
        },
        "pool_activity": {
            "swap_count": parse_u64_or_zero(minute_row.get("swap_count")),
            "volume_raw": parse_string_or(minute_row.get("volume_raw"), "0"),
            "unique_users_sum": parse_u64_or_zero(minute_row.get("unique_users_sum")),
            "min_slot": parse_u64_or_zero(minute_row.get("min_slot")),
            "max_slot": parse_u64_or_zero(minute_row.get("max_slot")),
            "last_ingested_unix_ms": parse_u64_or_zero(minute_row.get("last_ingested_unix_ms")),
        },
        "user_activity": {
            "active_users": parse_u64_or_zero(hour_row.get("active_users")),
            "claim_events": parse_u64_or_zero(hour_row.get("claim_events")),
            "fee_x_raw": parse_string_or(hour_row.get("fee_x_raw"), "0"),
            "fee_y_raw": parse_string_or(hour_row.get("fee_y_raw"), "0"),
        },
        "parse_health": {
            "events_total": parse_u64_or_zero(events_row.get("events_total")),
            "parsed_events": parse_u64_or_zero(events_row.get("parsed_events")),
            "parse_error_events": parse_u64_or_zero(events_row.get("parse_error_events")),
            "parse_warning_events": parse_u64_or_zero(events_row.get("parse_warning_events")),
        }
    }))
}

#[get("/v1/pools/{pool}/events")]
pub async fn v1_pool_events(
    path: web::Path<String>,
    query: web::Query<PoolEventsQuery>,
    state: web::Data<AppState>,
) -> impl Responder {
    state.metrics.inc_request();
    let pool = path.into_inner();
    if pool.trim().is_empty() {
        return bad_request(&state, "invalid_pool", "`pool` cannot be empty", None);
    }
    let limit = match validated_limit(&state, "limit", query.limit, 100, 1, 500) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if let Err(resp) = validate_slot_range(&state, query.from_slot, query.to_slot) {
        return resp;
    }
    let decoded_cursor = match decode_optional_cursor(&state, query.cursor.as_deref()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let silver_table = state.clickhouse.table_ref("silver_dlmm_events");
    let mut sql = format!(
        "SELECT
            slot,
            signature,
            instruction_index,
            inner_index,
            block_time_ms,
            event_name,
            user,
            ifNull(toString(amount_in_raw), '') AS amount_in_raw,
            amount_in_mint,
            token_x_mint,
            token_y_mint,
            swap_for_y,
            ifNull(toString(fee_x_raw), '') AS fee_x_raw,
            ifNull(toString(fee_y_raw), '') AS fee_y_raw,
            parse_error,
            parse_warning
        FROM {silver_table}
        WHERE ifNull(pool, '') = {}",
        sql_quote(&pool)
    );

    if let Some(user) = query.user.as_deref() {
        sql.push_str(&format!(" AND ifNull(user, '') = {}", sql_quote(user)));
    }
    if let Some(from_slot) = query.from_slot {
        sql.push_str(&format!(" AND slot >= {}", from_slot));
    }
    if let Some(to_slot) = query.to_slot {
        sql.push_str(&format!(" AND slot <= {}", to_slot));
    }
    let events = match parse_event_filter(&state, query.event.as_deref()) {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    append_event_in_predicate(&mut sql, &events);
    if let Some(cursor) = decoded_cursor.as_ref() {
        append_cursor_predicate(&mut sql, cursor);
    }

    sql.push_str(&format!(
        " ORDER BY slot DESC, signature DESC, instruction_index DESC, inner_index DESC LIMIT {}",
        limit + 1
    ));

    let rows = match query_rows_or_500(&state, &sql) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let mut items = rows
        .into_iter()
        .map(|row| PoolEventItem {
            slot: parse_u64_or_zero(row.get("slot")),
            signature: parse_string_or_empty(row.get("signature")),
            instruction_index: parse_u64_or_zero(row.get("instruction_index")) as u16,
            inner_index: parse_i64_or(row.get("inner_index"), -1) as i16,
            block_time_ms: parse_u64(row.get("block_time_ms")),
            event_name: parse_string_or(row.get("event_name"), "unknown"),
            user: parse_string(row.get("user")),
            amount_in_raw: parse_string(row.get("amount_in_raw")).filter(|v| !v.is_empty()),
            amount_in_mint: parse_string(row.get("amount_in_mint")),
            token_x_mint: parse_string(row.get("token_x_mint")),
            token_y_mint: parse_string(row.get("token_y_mint")),
            swap_for_y: parse_bool(row.get("swap_for_y")),
            fee_x_raw: parse_string(row.get("fee_x_raw")).filter(|v| !v.is_empty()),
            fee_y_raw: parse_string(row.get("fee_y_raw")).filter(|v| !v.is_empty()),
            parse_error: parse_string(row.get("parse_error")),
            parse_warning: parse_string(row.get("parse_warning")),
        })
        .collect::<Vec<_>>();

    let next_cursor = if items.len() > limit {
        let tail = items.pop();
        tail.map(|last| {
            encode_next_cursor(
                last.slot,
                last.signature,
                last.instruction_index,
                last.inner_index,
            )
        })
    } else {
        None
    };

    HttpResponse::Ok().json(PaginatedResponse {
        items,
        limit,
        next_cursor,
    })
}
