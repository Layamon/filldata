use futures::executor::block_on;

use clap::Parser;
use postgres::{Client, NoTls};
use schema::{AttrInfo, Table, TYPE_MAP};

mod schema;
mod typed_generator;

#[derive(Parser, Debug)]
#[command(
    author = "Layamon <sdwhlym@gmail.com>",
    version = "0.1.0",
    about = "This is a tools for generating random data for random table schema in PostgreSQL"
)]
struct Args {
    #[arg(long, default_value = "localhost")]
    hostname: String,
    #[arg(long, default_value = "5432")]
    port: String,
    #[arg(long, default_value = "tdb")]
    dbname: String,
    #[arg(long, default_value = "layamon")]
    user: String,
    #[arg(long, default_value = "mars3_t")]
    table: String,
    #[arg(long, default_value_t = 1)]
    parallelnum: i32,
    #[arg(long)]
    debug: bool,
    #[arg(long, default_value = "./genedata.log")]
    log: String,
    #[arg(long, default_value_t = 1000)]
    rows: u32,
    #[arg(long, default_value_t = 10)]
    batch: u32,
}

async fn load_data(
    rel_info: &mut Table<'_>,
    args: &Args,
    client: &mut Client,
) -> Result<u64, Box<dyn std::error::Error>> {
    let insert_stmt = rel_info.generate_insertbatch(&args);
    let rows_affected = client.execute(&insert_stmt, &[])?;
    Ok(rows_affected)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    //println!("Hostname: {}", args.hostname);
    //println!("Port: {}", args.port);
    //println!("Database: {}", args.dbname);
    //println!("User: {}", args.user);
    //println!("Table: {}", args.table);
    //println!("Parallel Number: {}", args.parallelnum);
    //println!("Debug Mode: {}", args.debug);
    //println!("Log File: {}", args.log);
    //println!("Rows: {}", args.rows);
    //println!("Batch: {}", args.batch);

    // Connection details
    let database_url = format!(
        "host={} user={} port={} dbname={}",
        args.hostname, args.user, args.port, args.dbname
    );

    //println!("{}", database_url);

    // Connect to the PostgreSQL database
    let mut client = Client::connect(database_url.as_str(), NoTls)?;

    // Define the table name to retrieve schema for
    let table_name = args.table.to_string();

    // Query the pg_attribute catalog to get schema information
    let query = format!(
        "SELECT attname, atttypid, atttypmod \
         FROM pg_attribute \
         WHERE attrelid = '{}'::regclass AND attnum > 0 \
         ORDER BY attnum;",
        table_name
    );

    // Execute the query
    let mut rel_info = Table::default();
    rel_info.tablename = &args.table;
    for row in client.query(query.as_str(), &[])? {
        let mut attr_info = AttrInfo::default();
        attr_info.attname = row.get("attname");
        let tid: u32 = row.get("atttypid");
        match TYPE_MAP.get(&tid) {
            Some(typeinfo) => {
                attr_info.type_info = typeinfo.clone();
            }
            None => {
                println!("Error: unconfig type {}", tid);
            }
        }
        attr_info.typmod = row.get("atttypmod");

        rel_info.tids.push(attr_info);
    }
    //println!("{:?}", rel_info);

    let mut remain_rows = args.rows;
    while remain_rows > 0 {
        let insert_stmt = rel_info.generate_insertbatch(&args);
        let rows_affected = client.execute(&insert_stmt, &[])?;

        remain_rows -= rows_affected as u32;
    }

    //let rows_affected = block_on(load_data(&mut rel_info, &args, &mut client))?;

    Ok(())
}
