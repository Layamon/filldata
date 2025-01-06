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

#[derive(Parser, Debug)]
#[clap(author, version, about)]
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
    #[arg(long, value_enum, default_value_t = LoadMode::SingleThread)]
    loadmode: LoadMode,
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
    rel_info.tablename = args.table.clone();
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

    match args.loadmode {
        LoadMode::SingleThread => crate::st_load::load(&args, &mut rel_info),
        LoadMode::MultiThread => crate::mt_load::load(args, rel_info.clone()),
        LoadMode::Async => crate::async_load::load(&args, &mut rel_info),
    }

    Ok(())
}
