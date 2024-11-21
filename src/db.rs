use std::collections::HashMap;
use tokio::sync::{Mutex};
use std::sync::Arc;
use std::thread;
use std::net::SocketAddr;
use crate::resp::Value;
use std::time::{Duration, SystemTime};
use crate::config::Config;
type RedisConfig = Arc<Mutex<Config>>;
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

    pub async fn handle_command(&mut self, command: String,mut args: Vec<Value>,config:RedisConfig,addr:SocketAddr) -> Value {
        match command.to_lowercase().as_str() {
            "set" => {
                let mut config_lock=config.lock().await;
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
                            //这里插入过期时间
                            let key_str=match key.clone(){
                                Value::BulkString(Some(string)) => string,
                                _ => return Value::Error("Invalid key for SET".to_string()),
                            };
                            config_lock.set_expriations(key_str, expiration_time);
                            // self.expirations.insert(key.clone(), expiration_time);
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
                                        Err(_) => return Value::Error("Invalid TTL value for EX".to_string()),
                                    }
                                },
                                _ => return Value::Error("Invalid TTL value for EX".to_string()),
                            };
                            let expiration_time = SystemTime::now() + Duration::from_secs(ex as u64);
                            //这里插入过期时间
                            let key_str=match key.clone(){
                                Value::BulkString(Some(string)) => string,
                                _ => return Value::Error("Invalid key for SET".to_string()),
                            };
                            config_lock.set_expriations(key_str, expiration_time);
                            // self.expirations.insert(key.clone(), expiration_time);
                            args.remove(0); // Remove "EX"
                        },
                        _ => {
                            // If the command is not recognized, we can either ignore it or return an error.
                            return Value::Error(format!("Unknown option: {:?}", args[0]));
                        }
                    }
                }
                let key_str=match key{
                    Value::BulkString(Some(string)) => string,
                    _ => return Value::Error("Invalid key for SET".to_string()),
                };
                config_lock.set(key_str,value)
            }
            "get" => {
                if args.is_empty() {
                    return Value::Error("Wrong number of arguments for GET".to_string());
                }
                let key = args.remove(0);
                let key_str = match key {
                    Value::BulkString(Some(string)) => string,
                    _ => return Value::Error("Invalid key for GET".to_string()),
                };
                let mut config_lock=config.lock().await;
                config_lock.get(key_str)
            }
            "config" => {
                //增加config get的命令
                if args.is_empty() {
                    return Value::Error("Wrong number of arguments for KEYS".to_string());
                }
                let cmd = args.remove(0);
                match cmd{
                    Value::BulkString(Some(ref cmd)) if cmd.eq_ignore_ascii_case("get") => {
                        if args.len() == 1 {
                            let key = args.remove(0);
                            let key_string = match key {
                                Value::BulkString(Some(string)) => string,
                                _ => return  Value::Error("Invalid key for CONFIG GET".to_string())
                            };
                            let config_lock=config.lock().await;
                            config_lock.config_get(key_string)
                        } else {
                            Value::Error("Wrong number of arguments for CONFIG GET".to_string())
                        }
                    },
                    _ => Value::Error("Unknown CONFIG command".to_string()),
                }   
            }
            "keys" => {
                if args.is_empty() {
                    return Value::Error("Wrong number of arguments for KEYS".to_string());
                }
                let key = args.remove(0);
                match key{
                    Value::BulkString(Some(cmd)) => {
                        let config_lock=config.lock().await;
                        config_lock.get_keys(cmd)
                    },
                    _ => Value::Error("Unknown CONFIG command".to_string())
                }
            }
            "info" =>{
                if args.is_empty() {
                    return Value::Error("Wrong number of arguments for KEYS".to_string());
                }
                let cmd = args.remove(0);
                match cmd{
                    Value::BulkString(Some(ref cmd)) if cmd.eq_ignore_ascii_case("replication") => {
                        let config_lock=config.lock().await;
                        config_lock.get_info_replication()
                    },
                    _ => Value::Error("Unknown INFO command".to_string()),
                }  
            }
            "replconf" => {
                if args.is_empty() {
                    return Value::Error("Wrong number of arguments for REPLCONF".to_string());
                }
                let cmd = args.remove(0);
                match cmd{
                    Value::BulkString(Some(ref cmd)) if cmd.eq_ignore_ascii_case("listening-port") => {
                        if args.len() == 1 {
                            let port = args.remove(0);
                            // TODO: handle port for master
                            let mut config_lock=config.lock().await;
                            let slave_addr= format!("{}:{}",addr.clone().ip().to_string() , port);
                            config_lock.new_slave_come(addr.clone().to_string(),slave_addr).await;

                            Value::SimpleString("OK".to_string())
                        } else {
                            Value::Error("Wrong number of arguments for listening-port".to_string())
                        }
                    },
                    Value::BulkString(Some(ref cmd)) if cmd.eq_ignore_ascii_case("capa") => {
                        if args.len() == 1 {
                            let _arg1 = args.remove(0);
                            // TODO: handle psync2 mode
                            Value::SimpleString("OK".to_string())
                        } else {
                            Value::Error("Wrong number of arguments for listening-port".to_string())
                        }
                    },
                    _ => Value::Error("Unknown REPLCONF command".to_string()),
                }
            }
            "psync" =>{
                if args.is_empty() {
                    return Value::Error("Wrong number of arguments for REPLCONF".to_string());
                }
                let cmd = args.remove(0);
                // 处理第一个参数，第一次发送的话
                match cmd{
                    Value::BulkString(Some(ref cmd)) if cmd.eq_ignore_ascii_case("?") => {
                        if args.len() == 1 {
                            let _arg1 = args.remove(0);
                            // TODO: handle port for master
                            let mut config_lock=config.lock().await;
                            let mode = "FULLRESYNC".to_string();
                            let repl_id = match config_lock.get_key_info_of_replication("master_replid".to_string()){
                                Value::SimpleString(s) => s,
                                _ => "Unknown".to_string()
                            };
                            let master_repl_offset = match config_lock.get_key_info_of_replication("master_repl_offset".to_string()){
                                Value::Integer(s) => s.to_string(),
                                _ => "Unknown".to_string()
                            };
                            let response = format!("{} {} {}",mode,repl_id,master_repl_offset);

                            //开始增加一个handler发送文件给从机
                            config_lock.add_slave_resphandler(addr.to_string()).await;

                            Value::SimpleString(response)
                            //回复一个参数
                        } else {
                            Value::Error("Wrong number of arguments for PSYNC".to_string())
                        }
                    },
                    _ => Value::Error("Unknown REPLCONF command".to_string()),
                }
            }
            "type" => {
                if args.is_empty(){
                    return Value::Error("Wrong number of arguments for TYPE".to_string());
                }
                let value = args.remove(0);
                let value_str=match value{
                    Value::BulkString(Some(s))=> s,
                    _ => return Value::Error("Invalid value for TYPE".to_string()),
                };
        
                let mut config_lock=config.lock().await;
                let res = match config_lock.get(value_str){
                    Value::BulkString(Some(_))=> "string".to_string(),
                    Value::BulkString(None)=> "none".to_string(),
                    _ => "".to_string(),
                };
                Value::SimpleString(res)
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
