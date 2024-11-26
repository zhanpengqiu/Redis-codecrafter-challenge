use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::resp::Value;
use anyhow::anyhow;
use anyhow::Result;

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
        let mut sequence = 0;
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