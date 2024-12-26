pub mod generator {
    use chrono::{DateTime, Duration, Utc};
    use rand::{Rng, RngCore};
    use std::u32;

    #[derive(Debug, Default)]
    pub struct Generator {
        rng: rand::rngs::ThreadRng,
    }

    impl Generator {
        pub fn get_text(&mut self, maxlength: i32, _tid: &u32) -> String {
            const CHARSET: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

            let len = self.rng.gen_range(0..maxlength);

            (0..len)
                .map(|_| {
                    let idx = self.rng.gen_range(0..CHARSET.len());
                    CHARSET.chars().nth(idx).unwrap()
                })
                .collect()
        }
        pub fn get_int(&mut self, _tid: &u32) -> String {
            (self.rng.next_u32() % 1000).to_string()
        }
        pub fn get_float(&mut self, _tid: &u32) -> String {
            let a = self.rng.gen_range(0..99) as f32;
            (a / 100.0).to_string()
        }
        pub fn get_bool(&mut self, _tid: &u32) -> String {
            self.rng.gen_bool(0.5).to_string()
        }
        pub fn get_time(&mut self, _tid: &u32) -> String {
            let now: DateTime<Utc> = Utc::now();

            let two_days_ago = now - Duration::hours(48);

            let time_string = two_days_ago.format("%Y-%m-%dT%H:%M:%SZ").to_string();

            time_string
        }
        pub fn get_json(&mut self, _tid: &u32) -> String {
            let mut ret = String::from('{');
            for v in 0..10 {
                let key = self.get_text(3, _tid);
                ret.push_str(&format!("\"{}\":{},", key, v));
            }
            if let Some(_) = ret.pop() {
                // Replace the last character with '}'
                ret.push('}');
            }

            ret
        }
    }
}
