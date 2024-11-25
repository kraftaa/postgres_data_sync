use sqlx::{query, PgPool, Result, Row};
use std::collections::HashMap;
use std::env;
use std::error::Error;
use async_std::stream::StreamExt;
use chrono::NaiveDateTime;

async fn check_columns_exist(pool: &PgPool, table_name: &str) -> Result<(bool, bool, bool, Option<NaiveDateTime>)> {
    // Check if the column `created_at`/`updated_at'/`id' exists in the table
    let column_exists_query = format!(
        "SELECT column_name FROM information_schema.columns WHERE table_name = '{}' AND column_name IN ('created_at', 'updated_at', 'id')",
        table_name
    );
    println!("column_exists_query {:?}", &column_exists_query);

    let rows = sqlx::query(&column_exists_query)
        .fetch_all(pool)
        .await?;

    // Check if both columns are found
    let mut created_at_exists = false;
    let mut updated_at_exists = false;
    let mut id_exists = false;

    for row in rows {
        let column_name: String = row.try_get("column_name")?;
        if column_name == "created_at" {
            created_at_exists = true;
        } else if column_name == "updated_at" {
            updated_at_exists = true;
        } else if column_name == "id" {
            id_exists = true;
        }
    }
    let last_created_at: Option<NaiveDateTime> =  if created_at_exists {
        let query = format!("SELECT MAX(created_at) FROM {} WHERE created_at IS NOT NULL", table_name);
        let row = sqlx::query(&query)
            .fetch_one(pool)
            .await?;
        row.try_get::<Option<NaiveDateTime>, _>(0).ok().flatten()
    } else {
        None
    };

    Ok((created_at_exists, updated_at_exists, id_exists,last_created_at ))
}

async fn query_update(source_pool: &PgPool, target_pool: &PgPool, table_name: &str)  -> Result<String> {
    let (created_at_exists, updated_at_exists, id_exists, last_created_at) = check_columns_exist(source_pool, table_name).await?;

    let mut query = if created_at_exists && updated_at_exists {
        format!("SELECT * FROM {} WHERE created_at >= '{:?}' or updated_at >= '{:?}' ORDER BY created_at ASC", table_name, last_created_at.unwrap(), last_created_at.unwrap())
    } else if created_at_exists && !updated_at_exists {
        format!("SELECT * FROM {} WHERE created_at >= '{:?}' ORDER BY created_at ASC", table_name, last_created_at.unwrap())
    } else if !created_at_exists && !updated_at_exists && id_exists {
        format!("SELECT * FROM {} ORDER BY id ASC", table_name)
    } else {
        format!("SELECT * FROM {} ", table_name)
    };

    Ok(query)
}

async fn list_tables_create(source_pool: &PgPool, target_pool: &PgPool) -> Result<Vec<String>> {
    // Don't use it for now as trying on the single tables
    // Find tables which are not postgres own tables
    // let tables: Vec<String> = sqlx::query_scalar(
    //     "SELECT tablename
    //      FROM pg_catalog.pg_tables
    //      WHERE schemaname NOT IN ('pg_catalog', 'information_schema');"
    // )
    //     .fetch_all(&*source_pool)
    //     .await?;
    //
    // println!("Tables to transfer: {:?}", tables);

    // Store schema for each table from above
    let mut table_schemas: HashMap<String, String> = HashMap::new();
    let tables = vec!["table1".to_string(), "table2".to_string()];
    for table in &tables {
        // Query to get the schema definition from the source database
        // Need to create a temp table, then insert from there, as COPY doesn't support ON CONFLICT
        let temp_name = format!("TEMPORARY TABLE transform.{}_sqlx", &table);  //'CREATE TEMPORARY TABLE transform.{}_sqlx (' ||
        let physical_name = format!("TABLE IF NOT EXISTS public.{}", &table);  //'CREATE TABLE IF NOT EXISTS public.{} (' ||
        let schema_query_temp = format!(
            "SELECT
            'CREATE {} (' ||
            string_agg(
                column_name || ' ' ||
                data_type ||
                CASE
                    WHEN character_maximum_length IS NOT NULL THEN '(' || character_maximum_length || ')'
                    ELSE ''
                END ||
                CASE
                    WHEN is_nullable = 'NO' THEN ' NOT NULL'
                    ELSE ''
                END,
                ', '
            ) ||
            ');' AS create_table_statement
        FROM information_schema.columns
        WHERE table_name = '{}';" , temp_name, table
        );
        let schema_query_physical = format!(
            "SELECT
            'CREATE {} (' ||
            string_agg(
                column_name || ' ' ||
                data_type ||
                CASE
                    WHEN character_maximum_length IS NOT NULL THEN '(' || character_maximum_length || ')'
                    ELSE ''
                END ||
                CASE
                    WHEN is_nullable = 'NO' THEN ' NOT NULL'
                    ELSE ''
                END,
                ', '
            ) ||
            ');' AS create_table_statement
        FROM information_schema.columns
        WHERE table_name = '{}';" , physical_name, table
        );
        println!("{:?} schema_query" ,&schema_query_temp);

        match sqlx::query_as::<_, (String,)>(&schema_query_temp)
            .fetch_one(&*source_pool)
            .await {
            Ok((schema,)) => {
                table_schemas.insert(table.clone(), schema);
            }
            Err(e) => {
                eprintln!("Error fetching schema for table {}: {}", table, e);
            }
        }
    }

    // Recreate each table in target database (if not exists ) for both: physical table and update temp table
    for (table, schema) in &table_schemas {
        let table_load_physical = format!("public.{}", table); // will add later
        let table_load_sqlx = format!("transform.{}_sqlx", table);
        println!("Creating update table {} in target database ", table_load_sqlx);
        // let schema_with_check = schema.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS"); // for the cases when the table exists

        let check_table_query = format!("SELECT to_regclass('{}');", table_load_sqlx);
        let table_exists: Option<String> = sqlx::query_scalar(&check_table_query)
            .fetch_one(&*target_pool)
            .await
            .ok();

        if table_exists.is_none() {
            if let Err(e) = sqlx::query(schema).execute(&*target_pool).await {
                println!("Failed to create table {}: {}", table, e);
            } else {
                println!("Successfully created table {}.", table);
            }
        } else {
            println!("Table {} already exists. Skipping creation.", table);
        }
    }
    // won't do it for now as checking the load
    // for table in &tables {
    //     let index_query = format!(
    //         "SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = '{}';",
    //         table
    //     );
    //
    //     if let Ok(rows) = sqlx::query(&index_query).fetch_all(&*source_pool).await {
    //         for row in rows {
    //             let indexdef: String = row.get("indexdef");
    //             println!("Recreating index for table {}: {}", table, indexdef);
    //             if let Err(e) = sqlx::query(&indexdef).execute(&*target_pool).await {
    //                 println!("Failed to create index for table {}: {}", table, e);
    //             }
    //         }
    //     }
    // }
    //
    // // Fetch constraints for each table
    // for table in &tables {
    //     let constraint_query = format!(
    //         "SELECT conname, pg_catalog.pg_get_constraintdef(c.oid)
    //          FROM pg_catalog.pg_constraint c
    //          JOIN pg_catalog.pg_class t ON t.oid = c.conrelid
    //          JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
    //          WHERE n.nspname = 'public' AND t.relname = '{}';",
    //         table
    //     );
    //
    //     if let Ok(rows) = sqlx::query(&constraint_query).fetch_all(&*source_pool).await {
    //         for row in rows {
    //             let conname: String = row.get("conname");
    //             let condef: String = row.get("pg_catalog.pg_get_constraintdef");
    //             println!("Recreating constraint {} for table {}: {}", conname, table, condef);
    //             let alter_query = format!("ALTER TABLE {} ADD CONSTRAINT {} {}", table, conname, condef);
    //             if let Err(e) = sqlx::query(&alter_query).execute(&*target_pool).await {
    //                 println!("Failed to create constraint {} for table {}: {}", conname, table, e);
    //             }
    //         }
    //     }
    // }

    println!("All tables processed/recreated.");
    Ok((tables))
    // Ok(())
}

async fn transfer_table(
    source_pool: &PgPool,
    target_pool: &PgPool,
    table_name: &str,
    custom_query: &str,
) -> Result<(), Box<dyn Error>> {
    // Acquire a connection from sqlx pool (for non-COPY queries)
    let mut source_conn = source_pool.acquire().await?;
    let mut target_conn = target_pool.acquire().await?;
    // COPY OUT from the source database (streaming data) - passing custom query with conditions from query_update
    let mut copy_out = source_conn.copy_out_raw(&format!("COPY ({}) TO STDOUT WITH CSV HEADER", custom_query)).await?;

    // COPY IN to the target database (streaming data)
    let mut copy_in = target_conn.copy_in_raw(&format!("COPY {} FROM STDIN WITH CSV HEADER", table_name)).await?;

    // let mut buffer = vec![0; 8192]; // A buffer for chunking data
    // https://github.com/launchbadge/sqlx/issues/36
    // https://github.com/launchbadge/sqlx/blob/82d332f4b487440b4c2bd5d54a5f17dcc1abc92c/sqlx-postgres/src/copy.rs#L58
    while let Some(chunk) = copy_out.next().await {
        match chunk {
            Ok(data) => {
                // println!("data {:?}", &data);
                copy_in.send(data).await?; }
            Err(err) => {
                eprintln!("Error during streaming {:?}", err);
                return Err(Box::new(err));
            }
        }
    }
    // Finish the COPY operation on the target database
    copy_in.finish().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let postgres_url_source = env::var("POSTGRES_URL_SOURCE").unwrap_or_else(|_| "nothing here".to_string());
    let postgres_url_target = env::var("POSTGRES_URL_TARGET").unwrap_or_else(|_| "nothing here".to_string());

    let source_ssl = postgres_url_source.to_owned() + "?sslmode=require";
    let target_ssl = postgres_url_target.to_owned() + "?sslmode=require";
    // connect to the source
    let source_pool = PgPool::connect(source_ssl.as_str()).await?;
    // connect to the target
    let target_pool = PgPool::connect(target_ssl.as_str()).await?;

    // List of tables to transfer check with them
    // let tables = list_tables_create(&source_pool, &target_pool).await?;

    let tables = vec!["table1"]; // or to get it from the function list_tables_create?

    for table in tables {
        let custom_query = query_update(&source_pool, &target_pool, table).await?;
        transfer_table(&source_pool, &target_pool, table, &custom_query).await?;
    }

    Ok(())
}