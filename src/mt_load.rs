use postgres::{Client, NoTls};
use std::thread;

use crate::typed_generator::generator::Generator;

pub fn load(args: crate::Args, rel_info: crate::Table) {
    let database_url = format!(
        "host={} user={} port={} dbname={}",
        args.hostname, args.user, args.port, args.dbname
    );

    let database_url_clone = database_url.clone();
    let ri_clone = rel_info.clone();
    let handle = thread::spawn(move || {
        let mut client = match Client::connect(database_url_clone.as_str(), NoTls) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("{}", e);
                std::process::exit(1)
            }
        };

        let mut generator: Generator = Generator::default();

        let mut remain_rows = args.rows;
        while remain_rows > 0 {
            let insert_stmt = ri_clone.generate_insertbatch(&args, &mut generator);
            let rows_affected = match client.execute(&insert_stmt, &[]) {
                Ok(rows) => rows,
                Err(e) => {
                    eprintln!("{}", e);
                    0
                }
            };

            remain_rows -= rows_affected as u32;
        }
    });

    handle.join().unwrap();
}
