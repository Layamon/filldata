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
     3802u32 => TypeInfo::Json(16),
};

#[derive(Debug, Default)]
pub struct AttrInfo {
    pub attname: String,
    pub type_info: TypeInfo,
    pub typmod: i32,
}

#[derive(Debug, Default)]
pub struct Table<'a> {
    pub tablename: &'a str,
    pub tids: Vec<AttrInfo>,

    generator: Generator,
}

impl<'a> Table<'a> {
    pub fn generate_insertbatch(&mut self, args: &Args) -> String {
        let mut n = args.batch;
        let mut insert_stmt = format!("insert into {} values ", self.tablename);
        while n > 0 {
            insert_stmt.push_str(&self.generate_one_value(args));
            if n > 1 {
                insert_stmt.push(',');
            }

            n -= 1;
        }

        insert_stmt.push(';');

        insert_stmt
    }
    pub fn generate_one_value(&mut self, _args: &Args) -> String {
        let mut ret = String::new();

        ret.push('(');
        for (idx, attr) in self.tids.iter().enumerate() {
            match &attr.type_info {
                TypeInfo::Text(tid) => {
                    // varchar length = typmod - 4 in pg, 500 for text type.
                    let mut maxlength: i32 = 500;
                    if attr.typmod > 0 {
                        maxlength = attr.typmod - 4;
                    }
                    ret.push_str(&Self::quote_val(
                        '\'',
                        &self.generator.get_text(maxlength, tid),
                    ))
                }
                TypeInfo::Int(tid) => ret.push_str(&self.generator.get_int(tid)),
                TypeInfo::Float(tid) => ret.push_str(&self.generator.get_float(tid)),
                TypeInfo::Bool(tid) => ret.push_str(&self.generator.get_bool(tid)),
                TypeInfo::Time(tid) => {
                    ret.push_str(&Self::quote_val('\'', &self.generator.get_time(tid)))
                }
                TypeInfo::Json(tid) => ret.push_str(&self.generator.get_json(tid)),
            };

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
