use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::resp::Value;

pub struct RedisDb {
    data: HashMap<Value, Value>,
}

impl RedisDb {
    pub fn new() -> Self {
        RedisDb {
            data: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: Value, value: Value) -> Value {
        self.data.insert(key, value);
        Value::SimpleString("OK".to_string())
    }

    pub fn get(&self, key: Value) -> Value {
        match self.data.get(&key) {
            Some(value) => value.clone(),
            None => Value::BulkString(None),
        }
    }

    pub fn delete(&mut self, key: Value) -> Value {
        if self.data.remove(&key).is_some() {
            Value::Integer(1)
        } else {
            Value::Integer(0)
        }
    }

    pub fn exists(&self, key: Value) -> Value {
        if self.data.contains_key(&key) {
            Value::Integer(1)
        } else {
            Value::Integer(0)
        }
    }

    pub fn increment(&mut self, key: Value) -> Value {
        match self.data.get_mut(&key) {
            Some(Value::Integer(ref mut i)) => {
                *i += 1;
                Value::Integer(*i)
            }
            _ => Value::Error("Key does not exist or is not an integer".to_string()),
        }
    }

    pub fn decrement(&mut self, key: Value) -> Value {
        match self.data.get_mut(&key) {
            Some(Value::Integer(ref mut i)) => {
                *i -= 1;
                Value::Integer(*i)
            }
            _ => Value::Error("Key does not exist or is not an integer".to_string()),
        }
    }

    pub fn handle_command(&mut self, command: String, args: Vec<Value>) -> Value {
        match command.to_lowercase().as_str() {
            "set" => {
                if args.len() == 2 {
                    self.set(args.first().unwrap().clone(), args.get(1).unwrap().clone())
                } else {
                    Value::Error("Wrong number of arguments for SET".to_string())
                }
            }
            "get" => {
                if args.len() == 1 {
                    self.get(args.first().unwrap().clone())
                } else {
                    Value::Error("Wrong number of arguments for GET".to_string())
                }
            }
            "del" => {
                if args.len() == 1 {
                    self.delete(args.first().unwrap().clone())
                } else {
                    Value::Error("Wrong number of arguments for DEL".to_string())
                }
            }
            "exists" => {
                if args.len() == 1 {
                    self.exists(args.first().unwrap().clone())
                } else {
                    Value::Error("Wrong number of arguments for EXISTS".to_string())
                }
            }
            "incr" => {
                if args.len() == 1 {
                    self.increment(args.first().unwrap().clone())
                } else {
                    Value::Error("Wrong number of arguments for INCR".to_string())
                }
            }
            "decr" => {
                if args.len() == 1 {
                    self.decrement(args.first().unwrap().clone())
                } else {
                    Value::Error("Wrong number of arguments for DECR".to_string())
                }
            }
            "ping" => Value::SimpleString("PONG".to_string()),
            "echo" => {
                if args.len() == 1 {
                    args.first().unwrap().clone()
                } else {
                    Value::Error("Wrong number of arguments for ECHO".to_string())
                }
            }
            _ => Value::Error(format!("Unknown command: {}", command)),
        }
    }
}
