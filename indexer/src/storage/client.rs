use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use serde::Serialize;

use super::models::BatchError;

#[derive(Debug, Clone)]
pub(super) struct ClickHouseHttpClient {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: String,
    timeout: Duration,
}

impl ClickHouseHttpClient {
    pub(super) fn from_env(database: String) -> Result<Self, BatchError> {
        let raw_url = env::var("CLICKHOUSE_URL").unwrap();
        let (host, port) = parse_clickhouse_url(&raw_url)?;
        let user = env::var("CLICKHOUSE_USER").unwrap();
        let password = env::var("CLICKHOUSE_PASSWORD").unwrap();
        let timeout_ms = env::var("CLICKHOUSE_TIMEOUT_MS")
            .unwrap()
            .parse::<u64>()
            .unwrap();

        Ok(Self {
            host,
            port,
            database,
            user,
            password,
            timeout: Duration::from_millis(timeout_ms),
        })
    }

    pub(super) fn database(&self) -> &str {
        &self.database
    }

    pub(super) fn execute_query(&self, query: &str) -> Result<(), BatchError> {
        self.send_post(query, "")
    }

    pub(super) fn insert_json_rows<T: Serialize>(
        &self,
        table: &str,
        rows: &[T],
    ) -> Result<(), BatchError> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut body = String::new();
        for row in rows {
            let line = serde_json::to_string(row)
                .map_err(|err| BatchError::Db(format!("serialize insert row failed: {}", err)))?;
            body.push_str(&line);
            body.push('\n');
        }

        let query = format!("INSERT INTO {} FORMAT JSONEachRow", table);
        self.send_post(&query, &body)
    }

    fn send_post(&self, query: &str, body: &str) -> Result<(), BatchError> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .map_err(|err| BatchError::Db(format!("clickhouse connect failed: {}", err)))?;
        stream
            .set_read_timeout(Some(self.timeout))
            .map_err(|err| BatchError::Db(format!("set read timeout failed: {}", err)))?;
        stream
            .set_write_timeout(Some(self.timeout))
            .map_err(|err| BatchError::Db(format!("set write timeout failed: {}", err)))?;

        let mut path = format!(
            "/?database={}&query={}&user={}",
            percent_encode(&self.database),
            percent_encode(query),
            percent_encode(&self.user)
        );
        if !self.password.is_empty() {
            path.push_str("&password=");
            path.push_str(&percent_encode(&self.password));
        }

        let request = format!(
            "POST {} HTTP/1.1\r\nHost: {}:{}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            path,
            self.host,
            self.port,
            body.len(),
            body
        );

        stream
            .write_all(request.as_bytes())
            .and_then(|_| stream.flush())
            .map_err(|err| BatchError::Db(format!("clickhouse write failed: {}", err)))?;

        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .map_err(|err| BatchError::Db(format!("clickhouse read failed: {}", err)))?;

        let mut lines = response.lines();
        let status_line = lines.next().unwrap_or_default().to_string();
        let status_ok = status_line.contains(" 200 ") || status_line.contains(" 204 ");
        if status_ok {
            return Ok(());
        }

        let body_start = response
            .find("\r\n\r\n")
            .map(|idx| idx + 4)
            .unwrap_or(response.len());
        let body_text = &response[body_start..];
        Err(BatchError::Db(format!(
            "clickhouse query failed: {} | {}",
            status_line,
            body_text.trim()
        )))
    }
}
fn parse_clickhouse_url(url: &str) -> Result<(String, u16), BatchError> {
    let trimmed = url.trim();
    let no_scheme = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))
        .unwrap_or(trimmed);

    let host_port = no_scheme.split('/').next().unwrap_or(no_scheme);
    let mut parts = host_port.split(':');
    let host = parts.next().unwrap_or("127.0.0.1").trim();
    let port = parts
        .next()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(8123);

    if host.is_empty() {
        return Err(BatchError::Db("invalid CLICKHOUSE_URL host".to_string()));
    }

    Ok((host.to_string(), port))
}

fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(hex_digit((b >> 4) & 0x0f));
            out.push(hex_digit(b & 0x0f));
        }
    }
    out
}

fn hex_digit(v: u8) -> char {
    match v {
        0..=9 => (b'0' + v) as char,
        _ => (b'A' + (v - 10)) as char,
    }
}
