
use sqlx::{PgPool, Row};
use serde_json::{json, Value};
use std::{env, fs, io};
use std::arch::aarch64::int8x8_t;
use std::error::Error;
use std::io::BufRead;
use chrono::{DateTime, NaiveDateTime, Utc};
use uuid::Uuid;
use sqlx::types::Json;
use bigdecimal::BigDecimal;
use dotenv::dotenv;
use sqlx::postgres::PgRow;
use sqlx::Column;
// in order to load all the data
// CREATE TABLE IF NOT EXISTS transform.table_name (
// id SERIAL PRIMARY KEY,
// data JSONB NOT NULL
// );
async fn process_files_and_insert(
    source_pool: &PgPool,
    target_pool: &PgPool,
    directory: &str
) -> Result<(), Box<dyn Error>> {
    // Iterate over SQL files in the directory
    let paths = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension() == Some(std::ffi::OsStr::new("txt")));

    for path in paths {
        let file_name = path.file_name().unwrap().to_str().unwrap();
        // The folder with txt files with column_name | data_type
        // Get table name from the file name (e.g., invoices.sql => invoices)
        let table_name = file_name.trim_end_matches(".txt");

        // Get column count and column names - don't need it anymore as there are only 2 columns for the data
        let (column_count, column_names) = count_columns_in_sql(&path).await?;
        println!("Table {} has {} columns: {:?}", table_name, column_count, column_names);

        let batch_size: i64 = 65000 / 2i64; // as i'm inserting into _data table with id and data
        println!("batch size  {} ", batch_size);
        // Fetch the last created_at timestamp from the source table if it exists (to query data since that timestamp)
        let last_created_at = if column_names.contains(&"created_at".to_string()) {
            get_last_created_at(target_pool, table_name).await?
        } else {
            None
        };
        println!("last created {:?} for a table {:?} ", &last_created_at, &table_name);

        let query = if column_names.contains(&"created_at".to_string()) || column_names.contains(&"updated_at".to_string()) {
            // Table has created_at or updated_at
            match last_created_at {
                Some(ref timestamp) => {
                    if batch_size > 0 {
                        format!(
                            "SELECT * FROM {} WHERE created_at >= '{}' OR updated_at >= '{}' ORDER BY created_at ASC LIMIT {}",
                            table_name, timestamp, timestamp, batch_size
                        )
                    } else {
                        format!("SELECT * FROM {} WHERE created_at >= '{}' OR updated_at >= '{}' ORDER BY created_at ASC ", table_name, timestamp, timestamp)
                    }
                }
                None => {
                    if batch_size > 0 {
                        format!("SELECT * FROM {} WHERE created_at IS NOT NULL ORDER BY created_at ASC LIMIT {}", table_name, batch_size)
                    } else {
                        format!("SELECT * FROM {} WHERE created_at IS NOT NULL ORDER BY created_at ASC", table_name)
                    }
                }
            }
        } else {
            // No created_at or updated_at -> select all rows
            if batch_size > 0 {
                format!("SELECT * FROM {} LIMIT {}", table_name, batch_size)
            } else {
                format!("SELECT * FROM {}", table_name)
            }
        };

        let rows = sqlx::query(&query)
            .fetch_all(source_pool)
            .await?;

        // Process and insert data as JSON into the target table
        for row in &rows {
            let json_data = handle_pg_row_as_jsonb(row).await;
            let normalized_table_name = format!("transform.{}_data", table_name);
            insert_json_data(target_pool, json_data, &normalized_table_name).await?;
        }
    }

    Ok(())
}
// don't need anymore as there are 2 columns now: id and data::jsonb
async fn count_columns_in_sql(file_path: &std::path::Path) -> Result<(usize, Vec<String>), Box<dyn Error>> {
    let file = fs::File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let in_table_definition = false;
    let mut column_names = Vec::new();

    for line in reader.lines() {
        let line = line?.trim().to_string();

        // Skip empty lines
        if line.is_empty() {
            continue;
        }

        // Split the line by '|' and extract column name
        let parts: Vec<&str> = line.split('|').collect();
        if !parts.is_empty() {
            let column_name = parts[0].trim().to_string();

            // skip columns that start with '_airbyte'
            if column_name.starts_with("_airbyte") {
                continue;
            }

            column_names.push(column_name);
        }
    }
    println!("columns len: {:?}", column_names.len());
    Ok((column_names.len(), column_names))
}

async fn get_last_created_at(pool: &PgPool, table_name: &str) -> Result<Option<String>, Box<dyn Error>> {
    let check_query = format!("
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{}'
        AND column_name = 'created_at';
    ", table_name);

    let column_exists = sqlx::query(&check_query)
        .fetch_optional(pool)
        .await?
        .is_some();

    if column_exists {
        let query = format!("SELECT MAX(created_at) FROM {} WHERE created_at IS NOT NULL", table_name);
        let row = sqlx::query(&query)
            .fetch_one(pool)
            .await?;

        let last_created_at: Option<NaiveDateTime> = row.try_get(0)?;
        Ok(last_created_at.map(|t| t.to_string()))
    } else {
        Ok(None)
    }
}
fn json_array_or_empty<T: serde::Serialize>(opt_vec: Option<Vec<T>>) -> Value {
    json!(opt_vec.unwrap_or_default())
}
async fn process_and_insert_data(
    source_pool: &PgPool,
    target_pool: &PgPool,
    table_name: &str,
    last_created_at: Option<String>,
) -> Result<(), Box<dyn Error>> {
    let query = match last_created_at {
        Some(timestamp) => format!("SELECT * FROM {} WHERE created_at >= '{}' or updated_at >= '{}'", table_name, timestamp, timestamp),
        None => format!("SELECT * FROM {} WHERE created_at IS NOT NULL", table_name),
    };

    let rows = sqlx::query(&query)
        .fetch_all(source_pool)
        .await?;
    let mut count = 0;
    for row in rows {
        count += 1;
        let json_data = handle_pg_row_as_jsonb(&row).await;
        let normalized_table_name = format!("transform.{}_data", table_name);
        insert_json_data(target_pool, json_data, &normalized_table_name).await?;
    }
    println!("count {:?}", count);
    Ok(())
}
// to handle all the types
async fn handle_pg_row_as_jsonb(row: &PgRow) -> Value {
    let mut json_data = serde_json::Map::new();

    for (i, column) in row.columns().iter().enumerate() {
        let column_name = column.name();
        let value: Value = match row.try_get::<Option<i32>, _>(i) {
            Ok(Some(v)) => json!(v), // i32
            Err(_) => match row.try_get::<Option<i16>, _>(i) { // Match smallint as i16
                Ok(Some(v)) => json!(v), // smallint
                Err(_) => match row.try_get::<Option<i64>, _>(i) {
                    Ok(Some(v)) => json!(v), // i64/ i32
                        Err(_) => match row.try_get::<Option<f64>, _>(i) {
                            Ok(Some(v)) => json!(v), // Handle FLOAT8 (f64)
                    Err(_) => match row.try_get::<Option<BigDecimal>, _>(i) {
                        Ok(Some(v)) => json!(v.to_string()), // BigDecimal
                        Err(_) => match row.try_get::<Option<DateTime<Utc>>, _>(i) {
                            Ok(Some(v)) => json!(v.to_rfc3339()), // Timestamp with timezone
                            Err(_) => match row.try_get::<Option<NaiveDateTime>, _>(i) {
                                Ok(Some(v)) => json!(v.to_string()), // Timestamp without timezone
                                Err(_) => match row.try_get::<Option<Uuid>, _>(i) {
                                    Ok(Some(v)) => json!(v.to_string()), // UUID
                                    Err(_) => match row.try_get::<Option<bool>, _>(i) {
                                        Ok(Some(v)) => json!(v), // bool
                                        Err(_) => match row.try_get::<Option<String>, _>(i) {
                                            Ok(Some(v)) => json!(v), // String
                                            Err(_) => match row.try_get::<Option<Json<Value>>, _>(i) {
                                                Ok(Some(v)) => v.0, // Unwrap the Json value
                                                Err(_) => match row.try_get::<Option<Json<Option<Value>>>, _>(i) {
                                                    Ok(Some(v)) => v.0.unwrap(), // Unwrap the Json value
                                                    Err(_) => match row.try_get::<Option<Vec<Json<Value>>>, _>(i) {
                                                        Ok(Some(v)) => {
                                                            // println!("value j{:?}", &v);
                                                            json!(v)
                                                        },
                                                      Err(_) => match row.try_get::<Option<Vec<String>>, _>(i) {
                                                          Ok(Some(v)) => {
                                                              // println!("value j{:?}", &v);
                                                              json!(v)
                                                          },
                                                        Err(_) => match row.try_get::<Vec<String>, _>(i) {
                                                            Ok(v) => {
                                                                // println!("value {:?}", &v);
                                                                json!(v)
                                                            },
                                                    Err(_) => match row.try_get::<Option<Vec<Json<Value>>>, _>(i) {
                                                        Ok(Some(v)) => {
                                                            // println!("value v {:?}", &v);
                                                            json!(v)
                                                        },
                                                    Err(_) => match row.try_get::<Option<Vec<Option<String>>>, _>(i) {
                                                        Ok(Some(v)) => {
                                                            // println!("value {:?}", &v);
                                                            json!(v)
                                                        },                                                            // a => json!(a.unwrap()),
                                                    Err(_) => match row.try_get::<Option<Vec<String>>, _>(i) {
                                                        Ok(Some(v)) => json!(v),
                                                        Ok(None) | Err(_) => json!(vec![] as Vec<String>),
                                                        // _ => Value::Null,
                                                    }
                                                        Err(_) => match row.try_get::<Option<Vec<i64>>, _>(i) {
                                                            Ok(v) => json_array_or_empty(v),
                                                            Ok(Some(_)) | Ok(None) | Err(_) => json!(vec![] as Vec<i64>),
                                                        Err(_) => match row.try_get::<Vec<i32>, _>(i) {
                                                            Ok(v) => json_array_or_empty(Some(v)),
                                                            Ok(_)  | Err(_) => json!(vec![] as Vec<i32>),
                                                            _ => Value::Null,
                                                        Err(_) => match row.try_get::<Option<Vec<i32>>, _>(i) {
                                                            Ok(v) => json_array_or_empty(v),
                                                            // Ok(Some(v)) => { println!("value {:?}", &v); json!(v)},
                                                            Ok(Some(_)) | Err(_) => json!(vec![] as Vec<i32>),
                                                            Err(_) => match row.try_get::<Option<Vec<Option<i32>>>, _>(i) {
                                                                Ok(Some(v)) => json!(v),
                                                                Err(_) => match row.try_get::<Vec<Option<i32>>, _>(i) {
                                                                    Ok(v) => json!(v),
                                                            _ => Value::Null,
                                                        } // Default to null if not found
                                                        _ => Value::Null,
                                                            },
                                                            _ => Value::Null,

                                                    },
                                                    _ => Value::Null,
                                                },
                                                _ => Value::Null,

                                                },
                                            _ => Value::Null, // Default to null if not found
                                        },
                                        _ => Value::Null, // Default to null if not found
                                    },
                                    _ => Value::Null, // Default to null if not found
                                                    },
                                                    _ => Value::Null,
                                                    },
                                                    _ => Value::Null,
                                },
                                _ => Value::Null, // Default to null if not found
                            },
                            _ => Value::Null, // Default to null if not found
                        },
                        _ => Value::Null, // Default to null if not found
                    },
                    _ => Value::Null,
                },
                _ => Value::Null,
            },
                        _ => Value::Null,
                    },
                    _ => Value::Null,
                },
                _ => Value::Null,
                        },
                        _ => Value::Null,
                    },
                    _ => Value::Null,
                },
                _ => Value::Null,
            },
            _ => Value::Null,
        };

        json_data.insert(column_name.to_string(), value);
    }

    Value::Object(json_data)
}

async fn insert_json_data(target_pool: &PgPool, json_data: Value, table_name: &str) -> Result<(), Box<dyn Error>> {
    let json_data = Json(json_data);
    // sqlx::query(&format!("TRUNCATE TABLE {}", table_name))
    //     .execute(target_pool)
    //     .await?;
    sqlx::query(&format!("INSERT INTO {} (data) VALUES ($1)", table_name))
        .bind(json_data)
        .execute(target_pool)
        .await?;

    Ok(())
}
const MAX_CELLS:i64 = 65_000; //max number of cells for postgres to insert
#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Connect to the database
    dotenv().ok();
    let postgres_url_source = env::var("POSTGRES_URL_SOURCE")?;
    let postgres_url_target = env::var("POSTGRES_URL_TARGET")?;
    let source_ssl = postgres_url_source.to_owned() + "?sslmode=require";
    let target_ssl = postgres_url_target.to_owned() + "?sslmode=require";
    let source_pool = PgPool::connect(source_ssl.as_str()).await?;
    let target_pool = PgPool::connect(target_ssl.as_str()).await?;
    // let source_pool = PgPool::connect("postgres://user:password@localhost/db_source").await?; // in case of localhost
    // let target_pool = PgPool::connect("postgres://user:password@localhost/db_target").await?; // in case of localhost

    println!("before process_files_and_insert");

    // Process and insert data -> in `txt_types` there are .txt files with column/types like
    // id      | integet
    // name    | varchar
    // address | text
    process_files_and_insert(&source_pool, &target_pool, "./txt_types").await?;


    println!("process_files_and_insert");

    Ok(())
}
