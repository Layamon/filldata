use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::typed_generator::generator::Generator;
use crate::Args;

use phf::phf_map;

#[derive(Debug, Clone)]
pub enum TypeInfo {
    Text(u32),
    Int(u32),
    Float(u32),
    Bool(u32),
    Time(u32),
    Json(u32),
}

impl Default for TypeInfo {
    fn default() -> Self {
        Self::Text(0)
    }
}

pub static TYPE_MAP: phf::Map<u32, TypeInfo> = phf_map! {
     1042u32 => TypeInfo::Text(1042),
     1043u32 => TypeInfo::Text(1043),
     25u32 => TypeInfo::Text(25),
     20u32 => TypeInfo::Int(20),
     23u32 => TypeInfo::Int(23),
     26u32 => TypeInfo::Int(26),
     27u32 => TypeInfo::Int(27),
     28u32 => TypeInfo::Int(28),
     29u32 => TypeInfo::Int(29),
     1700u32 => TypeInfo::Float(1700),
     701u32 => TypeInfo::Float(701),
     16u32 => TypeInfo::Bool(16),
     1114u32 => TypeInfo::Time(1114),
     1184u32 => TypeInfo::Time(1184),
     1082u32 => TypeInfo::Time(1082),
     3802u32 => TypeInfo::Json(3802),
     114u32 => TypeInfo::Json(114),
};

#[derive(Debug, Default, Clone)]
pub struct AttrInfo {
    pub attname: String,
    pub type_info: TypeInfo,
    pub typmod: i32,
}

#[derive(Debug, Default, Clone)]
pub struct Table {
    pub tablename: String,
    pub tids: Vec<AttrInfo>,
    pub unique_keys: Vec<String>,
}

impl Table {
    fn is_unique_column(&self, colname: &str) -> bool {
        self.unique_keys.iter().any(|k| k == colname)
    }

    fn get_unique_value(
        &self,
        attr: &AttrInfo,
        seq: u64,
    ) -> Option<String> {
        match &attr.type_info {
            TypeInfo::Text(_) => {
                let max_len = if attr.typmod > 0 {
                    (attr.typmod - 4) as usize
                } else {
                    500
                };
                let mut val = format!("u{}", seq);
                if val.len() > max_len {
                    val.truncate(max_len);
                }
                Some(Self::quote_val('\'', &val))
            }
            TypeInfo::Int(_) => Some(seq.to_string()),
            TypeInfo::Float(_) => Some(format!("{}.0", seq)),
            TypeInfo::Bool(_) => Some(if seq % 2 == 0 { "true" } else { "false" }.to_string()),
            TypeInfo::Time(_) => {
                let base = Generator::default_time_string();
                Some(Self::quote_val('\'', &format!("{}_{}", base, seq)))
            }
            TypeInfo::Json(_) => Some(Self::quote_val('\'', &format!("{{\"seq\":{}}}", seq))),
        }
    }
}

impl Table {
    pub fn generate_insertbatch(
        &self,
        args: &Args,
        generator: &mut Generator,
        count: u32,
        unique_seq: &AtomicU64,
    ) -> String {
        let mut n = count;
        let mut insert_stmt = format!("insert into {} values ", self.tablename);
        while n > 0 {
            insert_stmt.push_str(&self.generate_one_value(args, generator, unique_seq));
            if n > 1 {
                insert_stmt.push(',');
            }

            n -= 1;
        }

        insert_stmt.push(';');

        insert_stmt
    }
    pub fn generate_one_value(
        &self,
        _args: &Args,
        generator: &mut Generator,
        unique_seq: &AtomicU64,
    ) -> String {
        let mut ret = String::new();

        ret.push('(');
        for (idx, attr) in self.tids.iter().enumerate() {
            let val = if self.is_unique_column(&attr.attname) {
                let seq = unique_seq.fetch_add(1, Ordering::Relaxed);
                self.get_unique_value(attr, seq).unwrap_or_default()
            } else {
                match &attr.type_info {
                    TypeInfo::Text(tid) => {
                        let mut maxlength: i32 = 5;
                        if attr.typmod > 0 {
                            maxlength = attr.typmod - 4;
                        }
                        Self::quote_val('\'', &generator.get_text(maxlength, tid))
                    }
                    TypeInfo::Int(tid) => generator.get_int(tid),
                    TypeInfo::Float(tid) => generator.get_float(tid),
                    TypeInfo::Bool(tid) => generator.get_bool(tid),
                    TypeInfo::Time(tid) => {
                        Self::quote_val('\'', &generator.get_time(tid))
                    }
                    TypeInfo::Json(tid) => {
                        Self::quote_val('\'', &generator.get_json(tid))
                    }
                }
            };

            ret.push_str(&val);

            if idx == self.tids.len() - 1 {
                ret.push(')');
            } else {
                ret.push(',');
            }
        }

        ret
    }

    fn quote_val(c: char, s: &str) -> String {
        format!("{}{}{}", c, s, c)
    }
}
