use std::collections::HashMap;
use tokio::sync::{Mutex};
use std::sync::Arc;
use std::thread;
use std::net::SocketAddr;
use crate::resp::Value;
use std::time::{Duration, SystemTime};
use crate::config::Config;
use std::time::{Instant};
type RedisConfig = Arc<Mutex<Config>>;
use tokio::time::sleep;

#[derive(Clone, Debug,Eq, Hash, PartialEq,PartialOrd)]
pub struct RedisDb {
}

impl RedisDb {
    pub fn new() -> Self {
        RedisDb {
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
                let value_str = match value {
                    Value::BulkString(Some(string)) => {
                        match string.parse::<i64>() {
                            Ok(parsed_int) => Value::Integer(parsed_int),
                            Err(_) => Value::BulkString(Some(string)),
                        }
                    },
                    _ => Value::Error("Invalid value for SET".to_string()),
                };
                config_lock.set(key_str,value_str)
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
                let value = config_lock.get(key_str);
                match value{
                    Value::BulkString(Some(v)) => Value::BulkString(Some(v)),
                    Value::Integer(v) => Value::BulkString(Some(v.to_string())),
                    _ => Value::BulkString(None),
                }
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
                    Value::BulkString(Some(ref cmd)) if cmd.eq_ignore_ascii_case("getack") => {
                        if args.len() == 1 {
                            let _arg1 = args.remove(0);
                            let config_lock=config.lock().await;
                            let offset = config_lock.rcliinfo_get_slave_cmd_offset();

                            let mut reply_vec = Vec::new();
                            reply_vec.push(Value::BulkString(Some("REPLCONF".to_string())));
                            reply_vec.push(Value::BulkString(Some("ACK".to_string())));
                            reply_vec.push(Value::BulkString(Some(offset.to_string())));

                            Value::Array(reply_vec)
                        } else {
                            Value::Error("Wrong number of arguments for GETACK".to_string())
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
                            let config_lock=config.lock().await;
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
        
                let mut config_lock=config.lock().await;
                let res = config_lock.get_type(value);
                Value::SimpleString(res)
            }
            "xadd" => {
                if args.len() < 2 {
                    return Value::Error("Wrong number of arguments for XADD".to_string());
                }
                // 增加key到stream当中
                let stream_key = args.remove(0);
                let stream_value = args.remove(0);

                let mut hashmap = HashMap::new();
                while args.len()!=0 && args.len()%2 ==0{
                    let stream_content_key = args.remove(0);
                    let stream_content_value = args.remove(0);
                    hashmap.insert(stream_content_key, stream_content_value);
                    // 放入Stream当中
                }
                let mut config_lock=config.lock().await;
                match config_lock.xadd((stream_key, stream_value), hashmap).await{
                    Ok(res) => res,
                    Err(e) => Value::Error(format!("{}",e)),
                }

            }
            "xrange" => {
                if args.len() < 2 {
                    return Value::Error("Wrong number of arguments for XRANGE".to_string());
                }
                // 增加key到stream当中
                let stream_key = args.remove(0);
                let start = args.remove(0);
                let end = args.remove(0);
                let mut config_lock=config.lock().await;
                match config_lock.xrange(stream_key,start, end).await{
                    Ok(res) => res,
                    Err(e) => Value::Error(format!("{}",e)),
                }

            }
            "xread" => {
                //实现xread的逻辑
                if args.len() < 1 {
                    return Value::Error("Wrong number of arguments for XREAD".to_string());
                }
                let cmd = args.remove(0);
                match cmd {
                    Value::BulkString(Some(ref cmd)) if cmd.eq_ignore_ascii_case("block") => {
                        // 这里实现block 的循环检查
                        // 检查是否有新的item进来,进来了之后检查是不是当前想要的
                        let num_str = match args.remove(0){
                            Value::BulkString(Some(ref num_str)) => num_str.clone(),
                            _ => return Value::Error("Err block num".to_string())
                        };
                        let _ = args.remove(0); //这个得到的参数是streams

                        //在这里,阻塞读取
                        let start_time = Instant::now();
                        let interval = Duration::from_millis(20);
                        let block_num:u64= match num_str.parse::<i32>(){
                            Ok(num) => {
                                if num == 0 {
                                    u64::MAX
                                }else{
                                    num as u64
                                }
                            }
                            Err(e) => return Value::Error("Err block num".to_string())
                        };


                        let total_duration = Duration::from_millis(block_num as u64);
                        
                        let stream_key_num = args.len()/2;
                        let mut stream_key_vec = Vec::new();
                        let mut stream_name_vec = Vec::new();
                        for _ in 0..stream_key_num {
                            stream_key_vec.push(args.remove(0));
                        }
                        for _ in 0..stream_key_num { 
                            stream_name_vec.push(args.remove(0));
                        }
                        let mut stream_key_name_vec = Vec::new();
                        for i in 0..stream_key_num {
                            //在这里如果碰到stream_name_vec[i]是$的情况,那么就获取目前这个stream_key最大的一个项目
                            match stream_name_vec[i].clone(){
                                Value::BulkString(Some(ref s)) if s.eq_ignore_ascii_case("$") => {
                                    let mut config_lock=config.lock().await;
                                    let stream_name = match config_lock.xread_latest(stream_key_vec[i].clone()){
                                        Ok(res) => res,
                                        Err(e) => Value::Error(format!("{}",e)),
                                    };
                                    stream_key_name_vec.push((stream_key_vec[i].clone(), stream_name));
                                }
                                _ => {
                                    stream_key_name_vec.push((stream_key_vec[i].clone(), stream_name_vec[i].clone()));
                                }
                            }
                            
                        }

                        while start_time.elapsed() < total_duration {
                            //检查是否有新的项目进来并且项目是匹配的,没有的话继续执行,有的话打破循环
                            {                     
                                let mut config_lock=config.lock().await;
                                let res_val = match config_lock.xread(stream_key_name_vec.clone()).await{
                                    Ok(res) => res,
                                    Err(e) => Value::Error(format!("{}",e)),
                                };
                                match res_val.clone(){
                                    Value::Array(ref v) => {
                                        if !v.is_empty() {
                                            return res_val
                                        }
                                    }
                                    _=>{}
                                }
                            }
                            // 异步睡眠100ms
                            sleep(interval).await;
                        }
                        // 什么数据也没有就直接返回空的字符串
                        return Value::BulkString(None);
                    }
                    Value::BulkString(Some(ref cmd)) if cmd.eq_ignore_ascii_case("streams") => {
                        let stream_key_num = args.len()/2;
                        let mut stream_key_vec = Vec::new();
                        let mut stream_name_vec = Vec::new();
                        for _ in 0..stream_key_num {
                            stream_key_vec.push(args.remove(0));
                        }
                        for _ in 0..stream_key_num {
                            stream_name_vec.push(args.remove(0));
                        }
                        let mut stream_key_name_vec = Vec::new();
                        for i in 0..stream_key_num {
                            stream_key_name_vec.push((stream_key_vec[i].clone(), stream_name_vec[i].clone()));
                        }

                        let mut config_lock=config.lock().await;
                        match config_lock.xread(stream_key_name_vec).await{
                            Ok(res) => return res,
                            Err(e) => return Value::Error(format!("{}",e)),
                        }
                    }
                    _ => return Value::Error("Unknown XREAD command".to_string()),
                }

            }
            "incr" => {
                if args.is_empty() {
                    return Value::Error("Wrong number of arguments for GET".to_string());
                }
                let key = args.remove(0);
                let key_str = match key {
                    Value::BulkString(Some(string)) => string,
                    _ => return Value::Error("Invalid key for GET".to_string()),
                };
                {
                    let mut config_lock=config.lock().await;
                    let value = config_lock.get(key_str.clone());
                    match value { 
                        Value::Integer(n) => {
                            let incr_value = Value::Integer(n + 1);
                            config_lock.incr(key_str.clone(),incr_value.clone())
                        }
                        Value::BulkString(None) => {
                            let incr_value = Value::Integer(1);
                            config_lock.incr(key_str.clone(),incr_value.clone())
                        }
                        _ => Value::Error("ERR value is not an integer or out of range".to_string()),
                    }
                }

                
            }
            "wait" => {
                if args.len()!= 2 {
                    return Value::Error("Wrong number of arguments for WAIT".to_string());
                }

                let num_of_repl_slaves = match args.remove(0){
                    Value::BulkString(Some(ref num_str)) => num_str.clone(),
                    _ => return Value::Error("Err wait num".to_string())
                }.parse::<i32>().unwrap();

                
                let wait_num = match args.remove(0){
                    Value::BulkString(Some(ref num_str)) => num_str.clone(),
                    _ => return Value::Error("Err wait num".to_string())
                }.parse::<i32>().unwrap();
                println!("{}", wait_num);


                let start_time = Instant::now();
                let interval = Duration::from_millis(100);
                let total_duration = Duration::from_millis(wait_num as u64);
                let mut res_val = Value::BulkString(None);

                while start_time.elapsed() < total_duration {
                    //检查是否有新的项目进来并且项目是匹配的,没有的话继续执行,有的话打破循环
                    {                     
                        let mut config_lock=config.lock().await;
                        res_val = match config_lock.wait(num_of_repl_slaves.clone()).await{
                            Ok(res) => {
                                res_val = res.clone(); // 假设 res 是你需要的值
                            }
                            Err(e) => Value::Error(format!("{}",e)),
                        };
                    }
                    // 异步睡眠20ms
                    sleep(interval).await;
                }

                res_val
            }
            "ping" => Value::SimpleString("PONG".to_string()),
            "echo" => {
                if args.len() == 1 {
                    args.first().unwrap().clone()
                } else {
                    Value::Error("Wrong number of arguments for ECHO".to_string())
                }
            }
            "rdbfile"=>{
                Value::SimpleString("Get a RedisFile from master".to_string())
            }
            _ => Value::Error(format!("Unknown command: {}", command)),
        }
    }

}
