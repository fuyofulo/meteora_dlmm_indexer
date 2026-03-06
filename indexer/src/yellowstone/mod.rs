use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use futures::{SinkExt, StreamExt};
use serde_json::Value;
use yellowstone_grpc_proto::prelude::SubscribeUpdate;

use crate::parser::{ParsedUpdate, Parser};
use crate::storage::{BatchError, BatchWriter, DbInstructionRecord, DbRecord};

pub mod client;
pub mod subscriptions;

#[derive(Debug, Clone)]
struct ParsedDiscriminatorStat {
    name: String,
    count: u64,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct FailedParseKey {
    discriminator: Vec<u8>,
    name: String,
    error: String,
}

#[derive(Debug, Clone)]
struct FailedParseStat {
    count: u64,
    sample_signature: Option<String>,
    sample_slot: Option<u64>,
    sample_instruction_index: Option<u32>,
    sample_inner_index: Option<u32>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct ParseWarningKey {
    discriminator: Vec<u8>,
    name: String,
    warning: String,
}

#[derive(Debug, Clone)]
struct ParseWarningStat {
    count: u64,
    sample_signature: Option<String>,
    sample_slot: Option<u64>,
}

#[derive(Debug)]
struct RuntimeMetrics {
    started_at: Instant,
    last_report_at: Instant,
    last_total_updates: u64,
    last_dlmm_updates: u64,
    last_parsed_instructions: u64,
    last_failed_instructions: u64,
    db_enqueued: u64,
    db_dropped: u64,
    db_disconnected: u64,
}

impl RuntimeMetrics {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            started_at: now,
            last_report_at: now,
            last_total_updates: 0,
            last_dlmm_updates: 0,
            last_parsed_instructions: 0,
            last_failed_instructions: 0,
            db_enqueued: 0,
            db_dropped: 0,
            db_disconnected: 0,
        }
    }
}

#[derive(Debug, Default)]
struct ParseStats {
    total_updates: u64,
    dlmm_updates: u64,
    dlmm_updates_ok: u64,
    dlmm_updates_failed: u64,
    parsed_instructions: u64,
    failed_instructions: u64,
    unknown_discriminator_counts: HashMap<Vec<u8>, u64>,
    parsed_discriminator_counts: HashMap<Vec<u8>, ParsedDiscriminatorStat>,
    failed_parse_stats: HashMap<FailedParseKey, FailedParseStat>,
    parse_warning_stats: HashMap<ParseWarningKey, ParseWarningStat>,
}

impl ParseStats {
    fn record_program_instruction(
        &mut self,
        instruction: &Value,
        program_id: &str,
        signature: Option<&str>,
        slot: Option<u64>,
    ) {
        let program = instruction.get("program_id").and_then(Value::as_str);
        if program != Some(program_id) {
            return;
        }

        let discriminator_bytes = extract_discriminator(instruction);
        let is_unknown = instruction.get("error").and_then(Value::as_str)
            == Some("unknown instruction discriminator");

        if is_unknown && let Some(bytes) = discriminator_bytes.clone() {
            let entry = self.unknown_discriminator_counts.entry(bytes).or_insert(0);
            *entry += 1;
        }

        let is_parsed = instruction
            .get("parsed")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if is_parsed {
            if let Some(bytes) = discriminator_bytes.clone() {
                let name = instruction
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                let entry = self
                    .parsed_discriminator_counts
                    .entry(bytes)
                    .or_insert_with(|| ParsedDiscriminatorStat {
                        name: name.clone(),
                        count: 0,
                    });
                entry.count += 1;
                if entry.name == "unknown" && name != "unknown" {
                    entry.name = name;
                }
            }

            if let Some(warning) = instruction
                .get("warning")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
            {
                let discriminator = discriminator_bytes.unwrap_or_else(|| vec![0; 8]);
                let name = instruction
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                let key = ParseWarningKey {
                    discriminator,
                    name,
                    warning,
                };
                let entry =
                    self.parse_warning_stats
                        .entry(key)
                        .or_insert_with(|| ParseWarningStat {
                            count: 0,
                            sample_signature: signature.map(ToOwned::to_owned),
                            sample_slot: slot,
                        });
                entry.count += 1;
            }
            return;
        }

        let error = instruction
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("unknown parse error")
            .to_string();
        let name = instruction
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let discriminator = discriminator_bytes.unwrap_or_else(|| vec![0; 8]);
        let key = FailedParseKey {
            discriminator,
            name,
            error,
        };

        let entry = self
            .failed_parse_stats
            .entry(key)
            .or_insert_with(|| FailedParseStat {
                count: 0,
                sample_signature: signature.map(ToOwned::to_owned),
                sample_slot: slot,
                sample_instruction_index: instruction
                    .get("instruction_index")
                    .and_then(Value::as_u64)
                    .and_then(|v| u32::try_from(v).ok()),
                sample_inner_index: instruction
                    .get("inner_index")
                    .and_then(Value::as_u64)
                    .and_then(|v| u32::try_from(v).ok()),
            });
        entry.count += 1;
    }

    fn record_update(&mut self, parsed: &ParsedUpdate) {
        self.total_updates += 1;
        if parsed.dlmm_instruction_count() > 0 {
            self.dlmm_updates += 1;
            if parsed.failed_instructions() == 0 {
                self.dlmm_updates_ok += 1;
            } else {
                self.dlmm_updates_failed += 1;
            }
        }
        self.parsed_instructions += parsed.parsed_instructions();
        self.failed_instructions += parsed.failed_instructions();
    }
}

pub struct YellowstoneWorker {
    endpoint: String,
    x_token: Option<String>,
    batch_writer: BatchWriter,
}

impl YellowstoneWorker {
    pub fn new(endpoint: String, x_token: Option<String>, batch_writer: BatchWriter) -> Self {
        Self {
            endpoint,
            x_token,
            batch_writer,
        }
    }

    pub async fn run(self) {
        let endpoint = self.endpoint.clone();
        let x_token = self.x_token.clone();
        let parser = match Parser::new() {
            Ok(parser) => parser,
            Err(err) => {
                eprintln!("Failed to load parser: {}", err);
                return;
            }
        };

        let metrics_every = std::env::var("PARSER_METRICS_EVERY")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        let reconnect_ms = std::env::var("YELLOWSTONE_RECONNECT_MS")
            .unwrap()
            .parse::<u64>()
            .unwrap();
        println!("Yellowstone Worker started!");
        println!("Reconnect backoff (ms): {}", reconnect_ms);

        let mut parse_stats = ParseStats::default();
        let mut runtime_metrics = RuntimeMetrics::new();

        loop {
            println!("Connecting to {}...", endpoint);

            let mut client = match client::connect(&endpoint, x_token.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to connect to Yellowstone gRPC: {}", e);
                    tokio::time::sleep(Duration::from_millis(reconnect_ms)).await;
                    continue;
                }
            };

            println!("Connected to Yellowstone gRPC!");
            let request = subscriptions::create_subscription_request();

            let (mut subscribe_tx, mut stream) = match client.subscribe().await {
                Ok(res) => res,
                Err(e) => {
                    eprintln!("Failed to subscribe: {}", e);
                    tokio::time::sleep(Duration::from_millis(reconnect_ms)).await;
                    continue;
                }
            };

            if let Err(e) = subscribe_tx.send(request).await {
                eprintln!("Failed to send subscription request: {}", e);
                tokio::time::sleep(Duration::from_millis(reconnect_ms)).await;
                continue;
            }

            println!("Subscribed to updates! Waiting for data...");

            loop {
                match stream.next().await {
                    Some(Ok(update)) => {
                        self.log_update(
                            &parser,
                            update,
                            &mut parse_stats,
                            &mut runtime_metrics,
                            metrics_every,
                        );
                    }
                    Some(Err(e)) => {
                        eprintln!("Stream error: {}", e);
                        break;
                    }
                    None => {
                        println!("Stream ended");
                        break;
                    }
                }
            }

            println!("Reconnecting to Yellowstone in {} ms...", reconnect_ms);
            tokio::time::sleep(Duration::from_millis(reconnect_ms)).await;
        }
    }

    fn log_update(
        &self,
        parser: &Parser,
        update: SubscribeUpdate,
        parse_stats: &mut ParseStats,
        runtime_metrics: &mut RuntimeMetrics,
        metrics_every: u64,
    ) {
        let parsed = parser.parse_update(&update);
        let program_id = parser.program_id();
        let slot = parsed.payload().get("slot").and_then(Value::as_u64);
        let signature = parsed
            .payload()
            .get("signature")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);

        if let Some(instructions) = parsed
            .payload()
            .get("instructions")
            .and_then(Value::as_array)
        {
            for instruction in instructions {
                parse_stats.record_program_instruction(
                    instruction,
                    program_id,
                    signature.as_deref(),
                    slot,
                );
            }
        }

        parse_stats.record_update(&parsed);

        let record = build_db_record(&parsed, program_id);
        match self.batch_writer.send(record) {
            Ok(()) => runtime_metrics.db_enqueued += 1,
            Err(BatchError::QueueFull) => runtime_metrics.db_dropped += 1,
            Err(BatchError::QueueDisconnected) => runtime_metrics.db_disconnected += 1,
            Err(BatchError::Db(_)) => {}
        }

        if parse_stats.total_updates % metrics_every == 0 {
            println!(
                "Parse summary: updates total={} dlmm_updates={} ok={} failed={} no_dlmm={} | dlmm instructions parsed={} failed={}",
                parse_stats.total_updates,
                parse_stats.dlmm_updates,
                parse_stats.dlmm_updates_ok,
                parse_stats.dlmm_updates_failed,
                parse_stats
                    .total_updates
                    .saturating_sub(parse_stats.dlmm_updates),
                parse_stats.parsed_instructions,
                parse_stats.failed_instructions
            );

            let mut unknown_list = parse_stats
                .unknown_discriminator_counts
                .iter()
                .map(|(disc, count)| (disc.clone(), *count))
                .collect::<Vec<_>>();
            unknown_list.sort_by(|a, b| b.1.cmp(&a.1));
            unknown_list.truncate(10);
            println!("Unknown discriminators with counts: {:?}", unknown_list);

            let mut parsed_list = parse_stats
                .parsed_discriminator_counts
                .iter()
                .map(|(disc, stat)| (disc.clone(), stat.name.clone(), stat.count))
                .collect::<Vec<_>>();
            parsed_list.sort_by(|a, b| b.2.cmp(&a.2));
            parsed_list.truncate(15);
            println!("Parsed discriminators with counts: {:?}", parsed_list);

            let mut failed_list = parse_stats
                .failed_parse_stats
                .iter()
                .map(|(key, stat)| {
                    (
                        key.discriminator.clone(),
                        key.name.clone(),
                        key.error.clone(),
                        stat.count,
                        stat.sample_slot,
                        stat.sample_signature.clone(),
                        stat.sample_instruction_index,
                        stat.sample_inner_index,
                    )
                })
                .collect::<Vec<_>>();
            failed_list.sort_by(|a, b| b.3.cmp(&a.3));
            failed_list.truncate(10);
            println!("Failed parse reasons with sample: {:?}", failed_list);

            let mut warning_list = parse_stats
                .parse_warning_stats
                .iter()
                .map(|(key, stat)| {
                    (
                        key.discriminator.clone(),
                        key.name.clone(),
                        key.warning.clone(),
                        stat.count,
                        stat.sample_slot,
                        stat.sample_signature.clone(),
                    )
                })
                .collect::<Vec<_>>();
            warning_list.sort_by(|a, b| b.3.cmp(&a.3));
            warning_list.truncate(10);
            println!("Parse warnings with sample: {:?}", warning_list);

            print_runtime_metrics(
                runtime_metrics,
                parse_stats.total_updates,
                parse_stats.dlmm_updates,
                parse_stats.parsed_instructions,
                parse_stats.failed_instructions,
            );
        }
    }
}

fn print_runtime_metrics(
    runtime_metrics: &mut RuntimeMetrics,
    total_updates: u64,
    dlmm_updates: u64,
    parsed_instructions: u64,
    failed_instructions: u64,
) {
    let interval_secs = runtime_metrics
        .last_report_at
        .elapsed()
        .as_secs_f64()
        .max(1e-6);
    let uptime_secs = runtime_metrics.started_at.elapsed().as_secs_f64().max(1e-6);

    let interval_updates = total_updates.saturating_sub(runtime_metrics.last_total_updates);
    let interval_dlmm_updates = dlmm_updates.saturating_sub(runtime_metrics.last_dlmm_updates);
    let interval_parsed_instr =
        parsed_instructions.saturating_sub(runtime_metrics.last_parsed_instructions);
    let interval_failed_instr =
        failed_instructions.saturating_sub(runtime_metrics.last_failed_instructions);

    let updates_per_sec = interval_updates as f64 / interval_secs;
    let dlmm_updates_per_sec = interval_dlmm_updates as f64 / interval_secs;
    let parsed_instr_per_sec = interval_parsed_instr as f64 / interval_secs;
    let failed_instr_per_sec = interval_failed_instr as f64 / interval_secs;
    let avg_updates_per_sec = total_updates as f64 / uptime_secs;

    println!(
        "Ingest metrics: updates/s={:.2} dlmm_updates/s={:.2} parsed_instr/s={:.2} failed_instr/s={:.4} avg_updates/s={:.2} db_enqueued={} db_dropped={} db_disconnected={}",
        updates_per_sec,
        dlmm_updates_per_sec,
        parsed_instr_per_sec,
        failed_instr_per_sec,
        avg_updates_per_sec,
        runtime_metrics.db_enqueued,
        runtime_metrics.db_dropped,
        runtime_metrics.db_disconnected,
    );

    runtime_metrics.last_report_at = Instant::now();
    runtime_metrics.last_total_updates = total_updates;
    runtime_metrics.last_dlmm_updates = dlmm_updates;
    runtime_metrics.last_parsed_instructions = parsed_instructions;
    runtime_metrics.last_failed_instructions = failed_instructions;
}

fn build_db_record(parsed: &ParsedUpdate, program_id: &str) -> DbRecord {
    let payload = parsed.payload();

    let instructions = payload
        .get("instructions")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|instruction| build_instruction_record(instruction, parsed, program_id))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    DbRecord {
        update_type: parsed.update_type().to_string(),
        slot: parsed.slot(),
        signature: parsed.signature().map(ToOwned::to_owned),
        created_at: parsed.created_at().map(ToOwned::to_owned),
        parsed_ok: parsed.parsed_ok(),
        parsed_instructions: parsed.parsed_instructions(),
        failed_instructions: parsed.failed_instructions(),
        dlmm_instruction_count: parsed.dlmm_instruction_count(),
        status: payload
            .get("status")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        status_detail_json: payload
            .get("status_detail")
            .filter(|value| !value.is_null())
            .and_then(|value| serde_json::to_string(value).ok()),
        payload_json: serde_json::to_string(payload).unwrap_or_else(|_| "{}".to_string()),
        instructions,
    }
}

fn build_instruction_record(
    instruction: &Value,
    parsed: &ParsedUpdate,
    program_id: &str,
) -> Option<DbInstructionRecord> {
    let program = instruction.get("program_id").and_then(Value::as_str)?;
    if program != program_id {
        return None;
    }

    let discriminator = extract_discriminator(instruction);

    Some(DbInstructionRecord {
        slot: parsed.slot(),
        signature: parsed.signature().map(ToOwned::to_owned),
        instruction_index: instruction
            .get("instruction_index")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(0),
        inner_index: instruction
            .get("inner_index")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok()),
        is_inner: instruction
            .get("is_inner")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        program_id: program.to_string(),
        name: instruction
            .get("name")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        discriminator,
        parsed: instruction
            .get("parsed")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        error: instruction
            .get("error")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        warning: instruction
            .get("warning")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        args_json: instruction
            .get("args")
            .filter(|value| !value.is_null())
            .and_then(|value| serde_json::to_string(value).ok()),
        idl_accounts_json: instruction
            .get("idl_accounts")
            .filter(|value| !value.is_null())
            .and_then(|value| serde_json::to_string(value).ok()),
    })
}

fn extract_discriminator(instruction: &Value) -> Option<Vec<u8>> {
    if let Some(bytes) = instruction
        .get("discriminator")
        .and_then(Value::as_array)
        .map(|items| extract_u8_vec(items))
        .filter(|bytes| bytes.len() == 8)
    {
        return Some(bytes);
    }

    instruction
        .get("raw_data")
        .and_then(Value::as_array)
        .map(|raw| extract_u8_vec_with_limit(raw, 8))
        .filter(|bytes| bytes.len() == 8)
}

fn extract_u8_vec(items: &[Value]) -> Vec<u8> {
    extract_u8_vec_with_limit(items, items.len())
}

fn extract_u8_vec_with_limit(items: &[Value], limit: usize) -> Vec<u8> {
    items
        .iter()
        .take(limit)
        .filter_map(Value::as_u64)
        .filter_map(|value| u8::try_from(value).ok())
        .collect::<Vec<_>>()
}
