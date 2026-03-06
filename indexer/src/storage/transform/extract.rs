use serde::Serialize;
use serde_json::Value;

use super::super::models::DbInstructionRecord;

#[derive(Debug, Clone, Serialize)]
pub(super) struct InstructionContext {
    pub(super) pool: Option<String>,
    pub(super) user: Option<String>,
    pub(super) amount_in_raw: Option<u64>,
    pub(super) amount_in_mint: Option<String>,
    pub(super) token_x_mint: Option<String>,
    pub(super) token_y_mint: Option<String>,
    pub(super) swap_for_y: Option<bool>,
    pub(super) event_owner: Option<String>,
    pub(super) event_fee_x_raw: Option<u64>,
    pub(super) event_fee_y_raw: Option<u64>,
}

pub(super) fn parse_instruction_context(instruction: &DbInstructionRecord) -> InstructionContext {
    let args_value = instruction
        .args_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());
    let idl_accounts_value = instruction
        .idl_accounts_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());

    let pool = extract_pool(&args_value, &idl_accounts_value);
    let user = extract_user(&args_value, &idl_accounts_value);
    let token_x_mint = extract_token_x_mint(&idl_accounts_value);
    let token_y_mint = extract_token_y_mint(&idl_accounts_value);
    let swap_for_y = extract_swap_for_y(&args_value);
    let amount_in_raw = extract_u64_at_path(&args_value, &["amount_in"])
        .or_else(|| extract_u64_at_path(&args_value, &["event", "amount_in"]));
    let event_owner = extract_event_owner(&args_value);
    let event_fee_x_raw = extract_u64_at_path(&args_value, &["event", "fee_x"]);
    let event_fee_y_raw = extract_u64_at_path(&args_value, &["event", "fee_y"]);
    let amount_in_mint = match (swap_for_y, token_x_mint.clone(), token_y_mint.clone()) {
        (Some(true), Some(x), Some(_)) => Some(x),
        (Some(false), Some(_), Some(y)) => Some(y),
        _ => None,
    };

    InstructionContext {
        pool,
        user,
        amount_in_raw,
        amount_in_mint,
        token_x_mint,
        token_y_mint,
        swap_for_y,
        event_owner,
        event_fee_x_raw,
        event_fee_y_raw,
    }
}

pub(super) fn parse_created_at_ms(raw: Option<&str>) -> Option<u64> {
    let raw = raw?;
    let (seconds_raw, nanos_raw) = raw.split_once('.')?;
    let seconds = seconds_raw.parse::<u64>().ok()?;
    let nanos_str = nanos_raw.chars().take(9).collect::<String>();
    let mut nanos_norm = nanos_str;
    while nanos_norm.len() < 9 {
        nanos_norm.push('0');
    }
    let nanos = nanos_norm.parse::<u32>().ok()?;
    Some(seconds.saturating_mul(1000) + u64::from(nanos / 1_000_000))
}

fn extract_pool(args: &Option<Value>, idl_accounts: &Option<Value>) -> Option<String> {
    extract_string_at_path(args, &["event", "lb_pair"])
        .or_else(|| extract_account_pubkey_by_names(idl_accounts, &["lb_pair"]))
}

fn extract_user(args: &Option<Value>, idl_accounts: &Option<Value>) -> Option<String> {
    extract_string_at_path(args, &["event", "from"])
        .or_else(|| extract_string_at_path(args, &["event", "owner"]))
        .or_else(|| extract_account_pubkey_by_names(idl_accounts, &["user", "sender", "owner"]))
}

fn extract_event_owner(args: &Option<Value>) -> Option<String> {
    extract_string_at_path(args, &["event", "owner"])
}

fn extract_token_x_mint(idl_accounts: &Option<Value>) -> Option<String> {
    extract_account_pubkey_by_names(idl_accounts, &["token_x_mint"])
}

fn extract_token_y_mint(idl_accounts: &Option<Value>) -> Option<String> {
    extract_account_pubkey_by_names(idl_accounts, &["token_y_mint"])
}

fn extract_swap_for_y(args: &Option<Value>) -> Option<bool> {
    extract_bool_at_path(args, &["swap_for_y"])
        .or_else(|| extract_bool_at_path(args, &["event", "swap_for_y"]))
}

fn extract_string_at_path(value: &Option<Value>, path: &[&str]) -> Option<String> {
    let mut current = value.as_ref()?;
    for key in path {
        current = current.get(*key)?;
    }
    if let Some(s) = current.as_str() {
        return Some(s.to_string());
    }
    if let Some(n) = current.as_i64() {
        return Some(n.to_string());
    }
    if let Some(n) = current.as_u64() {
        return Some(n.to_string());
    }
    None
}

fn extract_u64_at_path(value: &Option<Value>, path: &[&str]) -> Option<u64> {
    let mut current = value.as_ref()?;
    for key in path {
        current = current.get(*key)?;
    }

    if let Some(v) = current.as_u64() {
        return Some(v);
    }
    if let Some(v) = current.as_i64()
        && v >= 0
    {
        return Some(v as u64);
    }
    if let Some(v) = current.as_f64()
        && v.is_finite()
        && v >= 0.0
        && v.fract() == 0.0
        && v <= (u64::MAX as f64)
    {
        return Some(v as u64);
    }
    if let Some(s) = current.as_str()
        && let Ok(v) = s.parse::<u64>()
    {
        return Some(v);
    }
    None
}

fn extract_bool_at_path(value: &Option<Value>, path: &[&str]) -> Option<bool> {
    let mut current = value.as_ref()?;
    for key in path {
        current = current.get(*key)?;
    }
    if let Some(v) = current.as_bool() {
        return Some(v);
    }
    if let Some(v) = current.as_u64() {
        return Some(v != 0);
    }
    if let Some(v) = current.as_i64() {
        return Some(v != 0);
    }
    if let Some(s) = current.as_str() {
        return match s {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        };
    }
    None
}

fn extract_account_pubkey_by_names(idl_accounts: &Option<Value>, names: &[&str]) -> Option<String> {
    let accounts = idl_accounts.as_ref()?.as_array()?;
    for account in accounts {
        let Some(name) = account.get("name").and_then(Value::as_str) else {
            continue;
        };
        if names.iter().any(|candidate| candidate == &name)
            && let Some(pubkey) = account.get("pubkey").and_then(Value::as_str)
        {
            return Some(pubkey.to_string());
        }
    }
    None
}
