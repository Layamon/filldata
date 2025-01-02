use std::str::FromStr;

use clap::{Parser, ValueEnum};
use postgres::{Client, NoTls};
use schema::{AttrInfo, Table, TYPE_MAP};

mod async_load;
mod mt_load;
mod schema;
mod st_load;
mod typed_generator;

#[derive(Debug, Clone, ValueEnum)]
enum LoadMode {
    SingleThread,
    MultiThread,
    Async,
}

impl FromStr for LoadMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "st" => Ok(LoadMode::SingleThread),
            "mt" => Ok(LoadMode::MultiThread),
            "async" => Ok(LoadMode::Async),
            _ => Err(format!("Invalid value for LoadMode: {}", s)),
        }
    }
}

impl std::fmt::Display for LoadMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                LoadMode::SingleThread => "SingleThread".to_string(),
                LoadMode::MultiThread => "MultiThread".to_string(),
                LoadMode::Async => "Async".to_string(),
            }
        )
    }
}

#[derive(Parser, Debug)]
#[command(
    author = "Layamon <sdwhlym@gmail.com>",
    version = "0.1.0",
    about = "This is a tools for generating
             random data for random table schema 
             in PostgreSQL"
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
    #[arg(long, default_value = "my_tbl")]
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
    #[arg(value_enum, default_value_t = LoadMode::SingleThread)]
    load_mode: LoadMode,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let query = format!(
        "SELECT attname, atttypid, atttypmod \
         FROM pg_attribute \
         WHERE attrelid = '{}'::regclass AND attnum > 0 \
         ORDER BY attnum;",
        args.table
    );

    let database_url = format!(
        "host={} user={} port={} dbname={}",
        args.hostname, args.user, args.port, args.dbname
    );

    let mut client = Client::connect(database_url.as_str(), NoTls)?;
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
    client.close()?;

    match args.load_mode {
        LoadMode::SingleThread => crate::st_load::load(&args, &mut rel_info),
        LoadMode::MultiThread => crate::mt_load::load(&args, &mut rel_info),
        LoadMode::Async => crate::async_load::load(&args, &mut rel_info),
    }

    Ok(())
}
