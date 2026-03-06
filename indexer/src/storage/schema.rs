use std::env;
use std::fs;

use super::client::ClickHouseHttpClient;
use super::models::BatchError;

const DEFAULT_SCHEMA_SQL: &str = include_str!("../../../schema/clickhouse_v2.sql");

fn split_sql_statements(raw_sql: &str) -> Vec<String> {
    let without_line_comments = raw_sql
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");

    without_line_comments
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn render_schema_for_database(schema_sql: &str, database: &str) -> String {
    let create_db_from = "CREATE DATABASE IF NOT EXISTS dune_project";
    let create_db_to = format!("CREATE DATABASE IF NOT EXISTS {}", database);
    schema_sql
        .replace(create_db_from, &create_db_to)
        .replace("dune_project.", &format!("{}.", database))
}

pub(super) fn init_schema(client: &ClickHouseHttpClient) -> Result<(), BatchError> {
    let raw_schema = if let Ok(path) = env::var("CLICKHOUSE_SCHEMA_PATH") {
        fs::read_to_string(&path)
            .map_err(|err| BatchError::Db(format!("failed to read schema {:?}: {}", path, err)))?
    } else {
        DEFAULT_SCHEMA_SQL.to_string()
    };
    let rendered_schema = render_schema_for_database(&raw_schema, client.database());

    for query in split_sql_statements(&rendered_schema) {
        client.execute_query(&query)?;
    }

    Ok(())
}
