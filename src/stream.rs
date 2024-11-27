use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::resp::Value;
use anyhow::anyhow;
use anyhow::Result;
use regex::Regex;

#[derive(Debug, Clone)]
pub struct Stream {
    id: HashMap<Value, Value>,
    data: HashMap<Value, HashMap<Value, Value>>,
    last_id: Option<Value>, // 用于存储最后一个条目的 ID
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            id: HashMap::new(),
            data: HashMap::new(),
            last_id: None,
        }
    }

    pub fn insert_stream_item(&mut self, (name_key, name_value): (Value, Value), entry: HashMap<Value, Value>) -> Result<Value> {
        let id = self.generate_id(name_value.clone());
        println!("{}", id);
        if self.data.is_empty() {
            if  id <= Value::BulkString(Some("0-0".to_string())) {
                return Err(anyhow!("ERR The ID specified in XADD must be greater than 0-0"));
            }
        } else {
            if id <= Value::BulkString(Some("0-0".to_string())) {
                return Err(anyhow!("ERR The ID specified in XADD must be greater than 0-0"));
            }
            let last_id = self.last_id.as_ref().unwrap().clone();
            if !self.is_valid_id(&id, &last_id) {
                return Err(anyhow!("ERR The ID specified in XADD is equal or smaller than the target stream top item")); 
            }
        }

        if self.data.contains_key(&id) {
            return Err(anyhow!("ID {:?} already exists", id.clone()));
        }

        self.id.insert(id.clone(), name_key.clone());
        self.data.insert(id.clone(), entry);
        self.last_id = Some(id.clone()); // 更新最后一个条目的 ID

        // 返回成功的 Result
        Ok(id)
    }

    pub fn xrange(&self, start_id: Value, end_id: Value) -> Result<Vec<Value>> {
        let mut entries = Vec::new();

        // 检查 start_id 和 end_id 是否包含 "-"
        if !contains_hyphen(&start_id) && !contains_hyphen(&end_id) {
            for (id, entry) in &self.data {
                let mut entry_array = Vec::new();
                let id_no_tail = remove_trailing(&id);
                
                if id_no_tail >= start_id && id_no_tail <= end_id {
                    entry_array.push(id.clone());
                    let mut array = Vec::new();
                    for item in entry.iter() {
                        let (key, value) = item;
                        array.push(key.clone());
                        array.push(value.clone());
                    }
                    entry_array.push(Value::Array(array));
                    entries.push(entry_array);
                }
            }
        } else if !contains_hyphen(&start_id) && contains_plus(&end_id){
            for (id, entry) in &self.data {
                let mut entry_array = Vec::new();
                let id_no_tail = remove_trailing(&id);
                
                if id_no_tail >= start_id && id_no_tail <= end_id {
                    entry_array.push(id.clone());
                    let mut array = Vec::new();
                    for item in entry.iter() {
                        let (key, value) = item;
                        array.push(key.clone());
                        array.push(value.clone());
                    }
                    entry_array.push(Value::Array(array));
                    entries.push(entry_array);
                }
            }
        }
        else if contains_hyphen(&start_id) && contains_plus(&end_id) {
            for (id, entry) in &self.data {
                let mut entry_array = Vec::new();
                let id_no_tail = remove_trailing(&id);
                
                if *id >= start_id{
                    entry_array.push(id.clone());
                    let mut array = Vec::new();
                    for item in entry.iter() {
                        let (key, value) = item;
                        array.push(key.clone());
                        array.push(value.clone());
                    }
                    entry_array.push(Value::Array(array));
                    entries.push(entry_array);
                }
            }
        }
        else {
            for (id, entry) in &self.data {
                let mut entry_array = Vec::new();
                
                if *id >= start_id && *id <= end_id {
                    entry_array.push(id.clone());
                    let mut array = Vec::new();
                    for item in entry.iter() {
                        let (key, value) = item;
                        array.push(key.clone());
                        array.push(value.clone());
                    }
                    entry_array.push(Value::Array(array));
                    entries.push(entry_array);
                }
            }
        }

        // 对 entries 进行排序
        entries.sort_by(|a, b| {
            let a_id = a.get(0).unwrap().clone();
            let b_id = b.get(0).unwrap().clone();
            compare_ids(a_id, b_id)
        });

        // 将排序后的 entries 转换为 result
        let result: Vec<Value> = entries.into_iter().map(|e| Value::Array(e)).collect();

        Ok(result)

    }


    fn generate_id(&self, id: Value) -> Value {
        match id {
            Value::BulkString(Some(ref s)) if s == "*" => {
                // 自动生成时间和序列号
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis();
                let sequence = 0;
                self.generate_unique_id(format!("{}", timestamp))
            }
            Value::BulkString(Some(ref s)) if s.ends_with('*') => {
                // 自动生成序列号
                let timestamp = s.trim_end_matches('*').trim_end_matches('-');
                self.generate_unique_id(format!("{}", timestamp))
            }
            Value::BulkString(Some(ref s)) => {
                // 显式指定 id
                Value::BulkString(Some(s.clone()))
            }
            _ => {
                // 处理其他类型的 Value
                Value::BulkString(Some(id.to_string()))
            }
        }
    }

    fn generate_unique_id(&self, mut base_id: String) -> Value {
        let mut sequence = if base_id=="0".to_string(){
            1
        }else{
            0
        };
        // let mut sequence = 0;
        loop {
            let id = format!("{}-{}", base_id, sequence);
            if !self.data.contains_key(&Value::BulkString(Some(id.clone()))) {
                return Value::BulkString(Some(id));
            }
            sequence += 1;
        }
    }

    fn is_valid_id(&self, new_id: &Value, last_id: &Value) -> bool {
        let (new_timestamp, new_sequence) = parse_id(new_id);
        let (last_timestamp, last_sequence) = parse_id(last_id);

        new_timestamp > last_timestamp || (new_timestamp == last_timestamp && new_sequence > last_sequence)
    }
}

fn parse_id(id: &Value) -> (u128, u64) {
    match id {
        Value::BulkString(Some(ref s)) => {
            let parts: Vec<&str> = s.split('-').collect();
            if parts.len() != 2 {
                panic!("Invalid ID format: {}", s);
            }
            let timestamp = parts[0].parse::<u128>().expect("Invalid timestamp");
            let sequence = parts[1].parse::<u64>().expect("Invalid sequence number");
            (timestamp, sequence)
        }
        _ => panic!("Invalid ID type"),
    }
}

fn remove_trailing(id: &Value) -> Value {
    match id {
        Value::BulkString(Some(ref s)) => {
            let re = Regex::new(r"-[0-9]+$").unwrap();
            println!("{:?}",s.trim_end_matches("-").to_string());
            Value::BulkString(Some(re.replace_all(s, "").to_string()))
        }
        _ => {Value::BulkString(Some("".to_string()))},
    }
}
fn contains_hyphen(id: &Value) -> bool {
    match id {
        Value::BulkString(Some(ref s)) => s.contains('-'),
        _ => false,
    }
}
fn contains_plus(id: &Value) -> bool {
    match id {
        Value::BulkString(Some(ref s)) => s.contains('+'),
        _ => false,
    }
}
// 辅助函数：比较两个 ID
fn compare_ids(a: Value, b: Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::BulkString(Some(a_str)), Value::BulkString(Some(b_str))) => {
            let (a_timestamp, a_sequence) = parse_id(&Value::BulkString(Some(a_str)));
            let (b_timestamp, b_sequence) = parse_id(&Value::BulkString(Some(b_str)));

            a_timestamp.cmp(&b_timestamp).then_with(|| a_sequence.cmp(&b_sequence))
        }
        _ => std::cmp::Ordering::Equal, // 如果类型不匹配，默认相等
    }
}