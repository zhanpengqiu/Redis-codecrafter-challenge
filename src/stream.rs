use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::resp::Value;
use anyhow::anyhow;
use anyhow::Result;
use regex::Regex;

#[derive(Debug, Clone)]
pub struct Stream {
    stream_items:HashMap<Value,HashMap<Value,HashMap<Value,Value>>>,
    last_ids: HashMap<Value, Value>, // 用于存储每个 name_key 的最后一个条目的 ID
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            stream_items: HashMap::new(),
            last_ids: HashMap::new(),
        }
    }

    pub fn insert_stream_item(&mut self, (name_key, name_value): (Value, Value), entry: HashMap<Value, Value>) -> Result<Value> {
        let id = self.generate_id(name_key.clone(),name_value.clone());
        println!("{}", id);

        if id <= Value::BulkString(Some("0-0".to_string())) {
            return Err(anyhow!("ERR The ID specified in XADD must be greater than 0-0"));
        }
        let last_id = self.last_ids.get(&name_key).cloned().unwrap_or_else(|| Value::BulkString(Some("0-0".to_string())));

        if !self.is_valid_id(&id, &last_id) {
            return Err(anyhow!("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
        }

        let stream_name_hashmap = self.stream_items.entry(name_key.clone()).or_insert_with(HashMap::new);

        if stream_name_hashmap.contains_key(&id) {
            return Err(anyhow!("ID {:?} already exists", id.clone()));
        }

        stream_name_hashmap.insert(id.clone(), entry.clone());

        self.last_ids.insert(name_key.clone(), id.clone()); // 更新最后一个条目的 I

        Ok(id)
    }
    pub fn xread(&mut self, streams: Vec<(Value, Value)>) -> Result<Vec<Value>> {
        let mut results = Vec::new();
        let mut stream_entries: HashMap<Value, Vec<Vec<Value>>> = HashMap::new();

        for (stream_name, stream_key) in streams.iter() {
            if let Some(stream_name_hashmap) = self.stream_items.get(stream_name) {
                let mut min_id = None;

                for (id, entry) in stream_name_hashmap.iter() {
                    if *id > *stream_key {
                        if min_id.is_none() || *id < min_id.clone().unwrap() {
                            min_id = Some(id.clone());
                        }
                    }
                }

                if let Some(min_id) = min_id {
                    if let Some(entry) = stream_name_hashmap.get(&min_id) {
                        let entry_array = vec![
                            min_id.clone(),
                            Value::Array(
                                entry.iter()
                                    .flat_map(|(k, v)| vec![k.clone(), v.clone()])
                                    .collect(),
                            ),
                        ];
                        stream_entries
                            .entry(stream_name.clone())
                            .or_insert_with(Vec::new)
                            .push(entry_array);
                    } 
                }
            }
        }

        for (stream_name, entries) in stream_entries {
            let stream_result = vec![
                stream_name,
                Value::Array(entries.into_iter().map(|e| Value::Array(e)).collect()),
            ];
            results.push(Value::Array(stream_result));
        }

        println!("{:?}", results);

        Ok(results)
    }

    pub fn xrange(&mut self, name_key:Value,start_id: Value, end_id: Value) -> Result<Vec<Value>> {
        let mut entries = Vec::new();

        let stream_name_hashmap = self.stream_items.entry(name_key.clone()).or_insert_with(HashMap::new);

        // 检查 start_id 和 end_id 是否包含 "-"
        if !contains_hyphen(&start_id) && !contains_hyphen(&end_id) {
            for (id, entry) in stream_name_hashmap {
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
            for (id, entry) in stream_name_hashmap {
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
            for (id, entry) in stream_name_hashmap {
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
            for (id, entry) in stream_name_hashmap {
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


    fn generate_id(&mut self, name_key:Value,id: Value) -> Value {
        match id {
            Value::BulkString(Some(ref s)) if s == "*" => {
                // 自动生成时间和序列号
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis();
                let sequence = 0;
                self.generate_unique_id(name_key,format!("{}", timestamp))
            }
            Value::BulkString(Some(ref s)) if s.ends_with('*') => {
                // 自动生成序列号
                let timestamp = s.trim_end_matches('*').trim_end_matches('-');
                self.generate_unique_id(name_key,format!("{}", timestamp))
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

    fn generate_unique_id(&mut self, name_key:Value,mut base_id: String) -> Value {
        let stream_name_hashmap = self.stream_items.entry(name_key.clone()).or_insert_with(HashMap::new);
        let mut sequence = if base_id=="0".to_string(){
            1
        }else{
            0
        };
        // let mut sequence = 0;
        loop {
            let id = format!("{}-{}", base_id, sequence);
            if !stream_name_hashmap.contains_key(&Value::BulkString(Some(id.clone()))) {
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