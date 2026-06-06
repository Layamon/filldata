use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use postgres::{Client, NoTls};

use crate::typed_generator::generator::Generator;

pub fn load(args: &crate::Args, rel_info: &mut crate::Table) {
    let database_url = format!(
        "host={} user={} port={} dbname={}",
        args.hostname, args.user, args.port, args.dbname
    );

    let mut client = match Client::connect(database_url.as_str(), NoTls) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(1)
        }
    };

    let mut generator: Generator = Generator::default();
    let unique_seq = Arc::new(AtomicU64::new(0));

    let mut remain_rows = args.rows;
    while remain_rows > 0 {
        let count = args.batch.min(remain_rows);
        let insert_stmt =
            rel_info.generate_insertbatch(&args, &mut generator, count, &unique_seq);
        if let Err(e) = client.execute(&insert_stmt, &[]) {
            eprintln!("{}", e);
        }
        remain_rows -= count;
    }
}
