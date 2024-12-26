use postgres::{Client, NoTls};

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

    let mut remain_rows = args.rows;
    while remain_rows > 0 {
        let insert_stmt = rel_info.generate_insertbatch(&args);
        let rows_affected = match client.execute(&insert_stmt, &[]) {
            Ok(rows) => rows,
            Err(e) => {
                eprintln!("{}", e);
                0
            }
        };

        remain_rows -= rows_affected as u32;
    }
}
