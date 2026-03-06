use actix_web::{HttpResponse, Responder, get, web};

use crate::models::{AppState, PaginatedResponse, SwapItem, SwapsQuery};
use crate::utils::{
    parse_i64_or, parse_string, parse_string_or, parse_string_or_empty, parse_u64_or_zero,
    sql_quote,
};
use crate::validation::{query_rows_or_500, validate_slot_range, validated_limit};

use super::{append_cursor_predicate, decode_optional_cursor, encode_next_cursor};

#[get("/v1/swaps")]
pub async fn v1_swaps(query: web::Query<SwapsQuery>, state: web::Data<AppState>) -> impl Responder {
    state.metrics.inc_request();
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
            event_name,
            pool,
            user,
            ifNull(toString(amount_in_raw), '') AS amount_in_raw,
            amount_in_mint,
            token_x_mint,
            token_y_mint
        FROM {silver_table}
        WHERE parsed = 1
          AND event_name IN ('swap', 'swap2', 'swap_exact_out2', 'event_cpi::Swap')"
    );

    if let Some(pool) = query.pool.as_deref() {
        sql.push_str(&format!(" AND ifNull(pool, '') = {}", sql_quote(pool)));
    }
    if let Some(user) = query.user.as_deref() {
        sql.push_str(&format!(" AND ifNull(user, '') = {}", sql_quote(user)));
    }
    if let Some(from_slot) = query.from_slot {
        sql.push_str(&format!(" AND slot >= {}", from_slot));
    }
    if let Some(to_slot) = query.to_slot {
        sql.push_str(&format!(" AND slot <= {}", to_slot));
    }
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
        .map(|row| SwapItem {
            slot: parse_u64_or_zero(row.get("slot")),
            signature: parse_string_or_empty(row.get("signature")),
            instruction_index: parse_u64_or_zero(row.get("instruction_index")) as u16,
            inner_index: parse_i64_or(row.get("inner_index"), -1) as i16,
            event_name: parse_string_or(row.get("event_name"), "unknown"),
            pool: parse_string(row.get("pool")),
            user: parse_string(row.get("user")),
            amount_in_raw: parse_string(row.get("amount_in_raw")).filter(|s| !s.is_empty()),
            amount_in_mint: parse_string(row.get("amount_in_mint")),
            token_x_mint: parse_string(row.get("token_x_mint")),
            token_y_mint: parse_string(row.get("token_y_mint")),
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
