use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::resp::Value;
use std::time::{Duration, SystemTime};

pub struct RedisDb {
    data: HashMap<Value, Value>,
    expirations: HashMap<Value, SystemTime>,
}

impl RedisDb {
    pub fn new() -> Self {
        RedisDb {
            data: HashMap::new(),
            expirations: HashMap::new(),
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

    pub fn handle_command(&mut self, command: String,mut args: Vec<Value>) -> Value {
        match command.to_lowercase().as_str() {
            "set" => {
                if args.len() <2{
                    Value::Error("Wrong number of arguments for SET".to_string());
                }
                let key = args.remove(0);
                let value = args.remove(0);
                
                while !args.is_empty() {
                    match args[0] {
                        Value::BulkString(Some(ref opt)) if opt.eq_ignore_ascii_case("PX") => {
                            if args.len() < 2 {
                                return Value::Error("Wrong number of arguments for PX".to_string());
                            }
                            let px = match args.remove(1) {
                                Value::BulkString(Some(ttl_ms_str)) => {
                                    match ttl_ms_str.parse::<u64>() {
                                        Ok(ttl_ms) => ttl_ms,
                                        Err(_) => return Value::Error("Invalid TTL value for PX".to_string()),
                                    }
                                },
                                _ => return Value::Error("Invalid TTL value for PX".to_string()),
                            };
                            let expiration_time = SystemTime::now() + Duration::from_millis(px as u64);
                            self.expirations.insert(key.clone(), expiration_time);
                            args.remove(0); // Remove "PX"
                        },
                        Value::BulkString(Some(ref opt)) if opt.eq_ignore_ascii_case("EX") => {
                            if args.len() < 2 {
                                return Value::Error("Wrong number of arguments for EX".to_string());
                            }
                            let ex = match args.remove(1) {
                                Value::BulkString(Some(ttl_secs_str)) => {
                                    match ttl_secs_str.parse::<u64>() {
                                        Ok(ttl_secs) => ttl_secs,
                                        Err(_) => return Value::Error("Invalid TTL value for PX".to_string()),
                                    }
                                },
                                _ => return Value::Error("Invalid TTL value for EX".to_string()),
                            };
                            let expiration_time = SystemTime::now() + Duration::from_secs(ex as u64);
                            self.expirations.insert(key.clone(), expiration_time);
                            args.remove(0); // Remove "EX"
                        },
                        _ => {
                            // If the command is not recognized, we can either ignore it or return an error.
                            return Value::Error(format!("Unknown option: {:?}", args[0]));
                        }
                    }
                }
                self.set(key,value)
            }
            "get" => {
                if args.is_empty() {
                    return Value::Error("Wrong number of arguments for GET".to_string());
                }
                let key = args.remove(0);

                // Check if the key has expired
                if let Some(expiration_time) = self.expirations.get(&key) {
                    if let Ok(now) = SystemTime::now().duration_since(*expiration_time) {
                        if now > Duration::from_secs(0) {
                            self.data.remove(&key);
                            self.expirations.remove(&key);
                            return Value::BulkString(None);
                        }
                    }
                }
                self.get(key)
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
