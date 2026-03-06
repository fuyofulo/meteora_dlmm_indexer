use std::collections::{HashMap, HashSet};

use super::super::models::{DbInstructionRecord, DbRecord};
use super::rows::{GoldPoolMinuteRow, GoldPoolUserHourRow, saturating_u128_to_u64};

#[derive(Default)]
pub(super) struct BatchAggregates {
    pub(super) total_updates: u64,
    pub(super) dlmm_updates: u64,
    pub(super) parsed_instructions: u64,
    pub(super) failed_instructions: u64,
    pub(super) unknown_discriminator_count: u64,
    pub(super) last_slot: u64,
    pool_minute: HashMap<(i64, String), PoolMinuteAgg>,
    pool_user_hour: HashMap<(i64, String, String), PoolUserHourAgg>,
}

#[derive(Default)]
struct PoolMinuteAgg {
    swap_count: u64,
    volume_raw: u128,
    users: HashSet<String>,
    min_slot: u64,
    max_slot: u64,
    initialized: bool,
    last_ingested_unix_ms: u64,
}

#[derive(Default)]
struct PoolUserHourAgg {
    swap_count: u64,
    volume_raw: u128,
    claim_events: u64,
    fee_x_raw: u128,
    fee_y_raw: u128,
    max_slot: u64,
    initialized: bool,
    last_ingested_unix_ms: u64,
}

pub(super) fn minute_bucket_from_ms(unix_ms: u64) -> i64 {
    (unix_ms / 60_000) as i64
}

fn hour_bucket_from_ms(unix_ms: u64) -> i64 {
    (unix_ms / 3_600_000) as i64
}

pub(super) fn is_swap_instruction(name: &str) -> bool {
    matches!(
        name,
        "swap" | "swap2" | "swap_exact_out2" | "event_cpi::Swap"
    )
}

pub(super) fn is_claim_event(name: &str) -> bool {
    matches!(
        name,
        "event_cpi::ClaimFee" | "event_cpi::ClaimFee2" | "claim_fee" | "claim_fee2"
    )
}

impl BatchAggregates {
    pub(super) fn record_update(&mut self, record: &DbRecord) {
        self.total_updates = self.total_updates.saturating_add(1);
        if record.dlmm_instruction_count > 0 {
            self.dlmm_updates = self.dlmm_updates.saturating_add(1);
        }
        self.parsed_instructions = self
            .parsed_instructions
            .saturating_add(record.parsed_instructions);
        self.failed_instructions = self
            .failed_instructions
            .saturating_add(record.failed_instructions);
        if let Some(slot) = record.slot {
            self.last_slot = self.last_slot.max(slot);
        }
    }

    pub(super) fn record_unknown_discriminator(&mut self, instruction: &DbInstructionRecord) {
        if instruction
            .error
            .as_deref()
            .map(|e| e.contains("unknown instruction discriminator"))
            .unwrap_or(false)
        {
            self.unknown_discriminator_count = self.unknown_discriminator_count.saturating_add(1);
        }
    }

    pub(super) fn record_swap(
        &mut self,
        pool: Option<&str>,
        user: Option<&str>,
        amount_in_raw: Option<u64>,
        slot: u64,
        record_ingested_ms: u64,
    ) {
        if let (Some(pool), Some(user)) = (pool, user) {
            let hour_bucket = hour_bucket_from_ms(record_ingested_ms);
            let key = (hour_bucket, pool.to_string(), user.to_string());
            let user_hour = self.pool_user_hour.entry(key).or_default();
            user_hour.swap_count = user_hour.swap_count.saturating_add(1);
            if let Some(v) = amount_in_raw {
                user_hour.volume_raw = user_hour.volume_raw.saturating_add(u128::from(v));
            }
            user_hour.max_slot = user_hour.max_slot.max(slot);
            user_hour.initialized = true;
            user_hour.last_ingested_unix_ms = record_ingested_ms;
        }

        if let Some(pool) = pool {
            let key = (minute_bucket_from_ms(record_ingested_ms), pool.to_string());
            let pool_minute = self.pool_minute.entry(key).or_default();
            pool_minute.swap_count = pool_minute.swap_count.saturating_add(1);
            if let Some(v) = amount_in_raw {
                pool_minute.volume_raw = pool_minute.volume_raw.saturating_add(u128::from(v));
            }
            if let Some(user) = user {
                pool_minute.users.insert(user.to_string());
            }
            if !pool_minute.initialized {
                pool_minute.min_slot = slot;
                pool_minute.max_slot = slot;
                pool_minute.initialized = true;
            } else {
                pool_minute.min_slot = pool_minute.min_slot.min(slot);
                pool_minute.max_slot = pool_minute.max_slot.max(slot);
            }
            pool_minute.last_ingested_unix_ms = record_ingested_ms;
        }
    }

    pub(super) fn record_claim(
        &mut self,
        pool: Option<&str>,
        owner: Option<&str>,
        fee_x_raw: Option<u64>,
        fee_y_raw: Option<u64>,
        slot: u64,
        record_ingested_ms: u64,
    ) {
        if let (Some(pool), Some(owner)) = (pool, owner) {
            let hour_bucket = hour_bucket_from_ms(record_ingested_ms);
            let key = (hour_bucket, pool.to_string(), owner.to_string());
            let user_hour = self.pool_user_hour.entry(key).or_default();
            user_hour.claim_events = user_hour.claim_events.saturating_add(1);
            if let Some(v) = fee_x_raw {
                user_hour.fee_x_raw = user_hour.fee_x_raw.saturating_add(u128::from(v));
            }
            if let Some(v) = fee_y_raw {
                user_hour.fee_y_raw = user_hour.fee_y_raw.saturating_add(u128::from(v));
            }
            user_hour.max_slot = user_hour.max_slot.max(slot);
            user_hour.initialized = true;
            user_hour.last_ingested_unix_ms = record_ingested_ms;
        }
    }

    pub(super) fn into_gold_rows(self) -> (Vec<GoldPoolMinuteRow>, Vec<GoldPoolUserHourRow>) {
        let gold_pool_minute = self
            .pool_minute
            .into_iter()
            .map(|((minute_bucket, pool), agg)| GoldPoolMinuteRow {
                minute_bucket,
                pool,
                swap_count: agg.swap_count,
                volume_raw: saturating_u128_to_u64(agg.volume_raw),
                unique_users: agg.users.len() as u64,
                min_slot: if agg.initialized { agg.min_slot } else { 0 },
                max_slot: if agg.initialized { agg.max_slot } else { 0 },
                last_ingested_unix_ms: agg.last_ingested_unix_ms,
            })
            .collect::<Vec<_>>();

        let gold_pool_user_hour = self
            .pool_user_hour
            .into_iter()
            .map(|((hour_bucket, pool, user), agg)| GoldPoolUserHourRow {
                hour_bucket,
                pool,
                user,
                swap_count: agg.swap_count,
                volume_raw: saturating_u128_to_u64(agg.volume_raw),
                claim_events: agg.claim_events,
                fee_x_raw: saturating_u128_to_u64(agg.fee_x_raw),
                fee_y_raw: saturating_u128_to_u64(agg.fee_y_raw),
                max_slot: if agg.initialized { agg.max_slot } else { 0 },
                last_ingested_unix_ms: agg.last_ingested_unix_ms,
            })
            .collect::<Vec<_>>();

        (gold_pool_minute, gold_pool_user_hour)
    }
}
