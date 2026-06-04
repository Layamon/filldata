use postgres::{Client, NoTls};
use std::thread;

use crate::typed_generator::generator::Generator;

pub fn load(args: crate::Args, rel_info: crate::Table) {
    let database_url = format!(
        "host={} user={} port={} dbname={}",
        args.hostname, args.user, args.port, args.dbname
    );

    let n_threads = args.parallelnum.max(1) as usize;
    let base_rows = args.rows / n_threads as u32;
    let remainder = args.rows % n_threads as u32;

    let mut handles = Vec::with_capacity(n_threads);

    for i in 0..n_threads {
        let thread_rows = base_rows + if (i as u32) < remainder { 1 } else { 0 };
        let database_url_clone = database_url.clone();
        let ri_clone = rel_info.clone();
        let args_clone = args.clone();

        let handle = thread::spawn(move || {
            let mut client = match Client::connect(database_url_clone.as_str(), NoTls) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("{}", e);
                    std::process::exit(1)
                }
            };

            let mut generator: Generator = Generator::default();

            let mut remain_rows = thread_rows;
            while remain_rows > 0 {
                let count = args_clone.batch.min(remain_rows);
                let insert_stmt = ri_clone.generate_insertbatch(&args_clone, &mut generator, count);
                if let Err(e) = client.execute(&insert_stmt, &[]) {
                    eprintln!("{}", e);
                }
                remain_rows -= count;
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
