mod aggregate;
mod extract;
mod rows;

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use super::client::ClickHouseHttpClient;
use super::models::{BatchError, DbRecord};
use aggregate::{BatchAggregates, is_claim_event, is_swap_instruction, minute_bucket_from_ms};
use extract::{InstructionContext, parse_created_at_ms, parse_instruction_context};
use rows::{BronzeRawUpdateRow, EventParts, GoldQualityMinuteRow, SilverDlmmEventRow};

#[derive(Default)]
pub(super) struct WriterState {
    pool_mints: HashMap<String, (String, String)>,
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn hydrate_pool_mints(writer_state: &mut WriterState, context: &mut InstructionContext) {
    if let (Some(pool_value), Some(token_x), Some(token_y)) = (
        context.pool.as_deref(),
        context.token_x_mint.as_deref(),
        context.token_y_mint.as_deref(),
    ) {
        writer_state.pool_mints.insert(
            pool_value.to_string(),
            (token_x.to_string(), token_y.to_string()),
        );
    }

    if let Some(pool_value) = context.pool.as_deref()
        && let Some((token_x, token_y)) = writer_state.pool_mints.get(pool_value)
    {
        if context.token_x_mint.is_none() {
            context.token_x_mint = Some(token_x.clone());
        }
        if context.token_y_mint.is_none() {
            context.token_y_mint = Some(token_y.clone());
        }
    }
}

fn fill_amount_in_mint(context: &mut InstructionContext) {
    if context.amount_in_mint.is_none() {
        context.amount_in_mint = match (
            context.swap_for_y,
            &context.token_x_mint,
            &context.token_y_mint,
        ) {
            (Some(true), Some(x), Some(_)) => Some(x.clone()),
            (Some(false), Some(_), Some(y)) => Some(y.clone()),
            _ => None,
        };
    }
}

pub(super) fn flush_batch(
    client: &ClickHouseHttpClient,
    batch: &[DbRecord],
    writer_state: &mut WriterState,
) -> Result<(), BatchError> {
    let mut analytics = BatchAggregates::default();
    let at_ms = now_unix_ms();
    let minute_bucket = minute_bucket_from_ms(at_ms);

    let mut bronze_updates = Vec::<BronzeRawUpdateRow>::with_capacity(batch.len());
    let mut silver_events = Vec::<SilverDlmmEventRow>::new();

    for record in batch {
        analytics.record_update(record);
        let record_ingested_ms = now_unix_ms();
        let created_at_ms = parse_created_at_ms(record.created_at.as_deref());

        let bronze_row = BronzeRawUpdateRow::from_record(record, record_ingested_ms);
        bronze_updates.push(bronze_row);

        for instruction in &record.instructions {
            let mut context = parse_instruction_context(instruction);
            hydrate_pool_mints(writer_state, &mut context);
            fill_amount_in_mint(&mut context);

            let event = EventParts::from_record_instruction(record, instruction);
            analytics.record_unknown_discriminator(instruction);

            if instruction.parsed && is_swap_instruction(&event.event_name) {
                analytics.record_swap(
                    context.pool.as_deref(),
                    context.user.as_deref(),
                    context.amount_in_raw,
                    event.slot,
                    record_ingested_ms,
                );
            }

            if instruction.parsed && is_claim_event(&event.event_name) {
                analytics.record_claim(
                    context.pool.as_deref(),
                    context.event_owner.as_deref(),
                    context.event_fee_x_raw,
                    context.event_fee_y_raw,
                    event.slot,
                    record_ingested_ms,
                );
            }

            let silver_row = SilverDlmmEventRow::from_parts(
                record_ingested_ms,
                created_at_ms,
                instruction,
                context,
                event,
            );
            silver_events.push(silver_row);
        }
    }

    client.insert_json_rows("bronze_raw_updates", &bronze_updates)?;
    client.insert_json_rows("silver_dlmm_events", &silver_events)?;

    let quality_row = GoldQualityMinuteRow {
        minute_bucket,
        total_updates: analytics.total_updates,
        dlmm_updates: analytics.dlmm_updates,
        parsed_instructions: analytics.parsed_instructions,
        failed_instructions: analytics.failed_instructions,
        unknown_discriminator_count: analytics.unknown_discriminator_count,
        last_slot: analytics.last_slot,
        last_ingested_unix_ms: at_ms,
    };

    let (gold_pool_minute, gold_pool_user_hour) = analytics.into_gold_rows();
    client.insert_json_rows("gold_pool_minute", &gold_pool_minute)?;
    client.insert_json_rows("gold_pool_user_hour", &gold_pool_user_hour)?;
    client.insert_json_rows("gold_quality_minute", &[quality_row])?;

    Ok(())
}
