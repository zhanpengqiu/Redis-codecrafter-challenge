use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::resp::Value;
use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub struct Config{
    rdbfile: HashMap<String, String>,
}
impl Config {
    pub fn new() -> Self {
        Config {
            rdbfile: HashMap::new(),
        }
    }
    pub fn insert(&mut self, name: String, value: String){
        self.rdbfile.insert(name, value);
    }
    pub fn get(&self, key: String) -> Value {
        match self.rdbfile.get(&key) {
            Some(value) => {
                let valuearray=Value::Array(vec![
                    Value::BulkString(Some(key.clone())),
                    Value::BulkString(Some(value.clone())),
                ]);
                valuearray
            },
            None => Value::BulkString(None),
        }
    }
}