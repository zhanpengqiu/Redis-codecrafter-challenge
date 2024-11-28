#![allow(unused_imports)]
mod resp;
mod db;
mod config;
mod duplication;
mod slave_stream;
mod stream;

use crate::resp::Value;
use crate::db::RedisDb;
use crate::slave_stream::Slaves;
use crate::config::Config;
use tokio::net::{TcpListener, TcpStream};
use std::net::{ToSocketAddrs, SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::Result;
use tokio::sync::Mutex;
use std::sync::{Arc};
use std::env;

type DataStore = RedisDb;
type RedisConfig = Arc<Mutex<Config>>;

#[tokio::main]
async fn main() {
    // 读取输入的命令
    let args: Vec<String> = env::args().collect();

    // 设置数据存储库和基础设置库
    let database = RedisDb::new();
    let redisconfig = Arc::new(Mutex::new(Config::new()));

    // 默认值
    let mut dir = "./".to_string();
    let mut dbfilename = "dump.rdb".to_string();
    let mut port = "6379".to_string();
    let mut replicaof = "".to_string();

    // 解析命令行参数并更新基础设置库
    if args.len() > 1 {
        for i in 0..args.len() - 1 {
            match args[i].as_str() {
                "--dir" => {
                    if i + 1 < args.len() {
                        dir = args[i + 1].clone();
                    }
                }
                "--dbfilename" => {
                    if i + 1 < args.len() {
                        dbfilename = args[i + 1].clone();
                    }
                }
                "--port" => {
                    if i + 1 < args.len() {
                        port = args[i + 1].clone();
                    }
                }
                "--replicaof"=>{
                    if i + 1 < args.len() {
                        let replicaof_ip_port = args[i + 1].clone();
                        let parts: Vec<&str> = replicaof_ip_port.split_whitespace().collect();
                        if parts.len() != 2 {
                            println!("Replicaof set failed");
                        }
                        let host = parts[0];
                        let port = parts[1];
                    
                        // 尝试将主机名解析为 IP 地址
                        match format!("{}:{}", host, port).to_socket_addrs() {
                            Ok(mut addrs) => {
                                if let Some(addr) = addrs.next() {
                                    replicaof=format!("{}:{}", addr.ip(), addr.port());
                                }
                            },
                            Err(_) => println!("to_socket_addrs failed"),
                        }
                    }
                }
                _ => {}
            }
        }
    }
    println!("{:?}",replicaof);
    // 更新配置
    {
        let mut config = redisconfig.lock().await;
        config.insert("dir".to_string(), dir.clone());
        config.insert("dbfilename".to_string(), dbfilename.clone());
        config.insert("port".to_string(), port.clone());
        if replicaof != "".to_string(){
            config.set_rcliinfo("role".to_string(), "slave".to_string());
        }
        config.load_rdb();
        config.slave_loop().await;
    }
    // 设置 IP 地址和端口
    let ip = "127.0.0.1".to_string();
    let ip_port = format!("{}:{}", ip, port);

    // 绑定监听地址
    let listener = TcpListener::bind(ip_port).await.unwrap();

    if !replicaof.is_empty() {
        let redisconfig_clone = Arc::clone(&redisconfig);
        let _ = perform_replication_handshake(&replicaof,database.clone(),redisconfig_clone).await;
    }

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => {
                    let redisconfig_clone = Arc::clone(&redisconfig);
                    tokio::spawn(async move {
                        let db = DataStore::new();
                        handle_conn(stream,db,redisconfig_clone).await
                        });
                }
                Err(e) => {
                    println!("error: {}", e);

                }
        }
    }
}

async fn handle_conn(stream: TcpStream, mut db: DataStore, redisconfig: RedisConfig) {
    let addr = stream.peer_addr().unwrap();
    let mut handler = resp::RespHandler::new(stream);
    println!("Starting read loop");
    let mut multi_cmd_flag = false;
    let mut multi_cmd_vec: Vec<(String, Vec<Value>)> = Vec::new();
    loop {
        let value = handler.read_value().await.unwrap();
        println!("Got value {:?}", value);

        // 提前声明变量
        let (command, args): (String, Vec<Value>);
        let mut response = Value::BulkString(None);

        if let Some(v) = value.clone() {
            let extracted = extract_command(v).unwrap();
            command = extracted.0; // 初始化变量
            args = extracted.1; // 初始化变量

            //检查command是不是multi
            match command.to_lowercase().as_str(){
                "multi" => {
                    multi_cmd_flag=true;
                    response = Value::SimpleString("OK".to_string());
                }
                "exec" => {
                    if multi_cmd_flag{
                        multi_cmd_flag=false;
                        let mut multi_cmd_response_vec = Vec::new();
                        for  (cmd, cmd_args) in &multi_cmd_vec {
                            response = db.handle_command(cmd.to_string(), cmd_args.clone(), redisconfig.clone(),addr).await;
                            multi_cmd_response_vec.push(response);
                        }
                        multi_cmd_vec.clear();
                        response = Value::Array(multi_cmd_response_vec);
                    }else{
                        response = Value::Error("ERR EXEC without MULTI".to_string());
                    }
                    
                }
                "discard" => {
                    if multi_cmd_flag{
                        multi_cmd_flag=false;
                        multi_cmd_vec.clear();
                        response = Value::SimpleString("OK".to_string());
                    }else{
                        response = Value::Error("ERR DISCARD without MULTI".to_string());
                    }
                }
                _ => {
                    if multi_cmd_flag{
                        multi_cmd_vec.push((command.clone(),args.clone()));
                        response = Value::SimpleString("QUEUED".to_string());
                    }else{
                        let respon = db.handle_command(command.clone(), args.clone(), redisconfig.clone(),addr).await;
                        response=respon
                    }
                }
            }
        }
        else{
            if multi_cmd_flag{
                continue;
            }else{
                break;
            }
        }
        println!("{:?}",response);
        handler.write_value(response).await.unwrap();
        // 记录处理的命令
        {
            let mut redisconfig_lock=redisconfig.lock().await;
            if let Some(v) = value.clone(){
                redisconfig_lock.rcliinfo_track_cmd(v).await;
            }
        }

        //处理同步信息
        match command.to_lowercase().as_str() {
            "psync" => {
                let mut redisconfig_lock=redisconfig.lock().await;
                redisconfig_lock.add_slave_resphandler(handler).await;
                break;
            }
            _ => {}
        };
    }
}
fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => {
            Ok((
                unpack_bulk_str(a.first().unwrap().clone())?,
                a.into_iter().skip(1).collect(),
            ))
        },
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}
fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(Some(s)) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string"))
    }
}

async fn perform_replication_handshake(replicaof: &str,mut db:DataStore,redisconfig: RedisConfig) -> Result<()> {
    // 尝试连接到主服务器
    let master_addr: SocketAddr = replicaof.parse().expect("Failed to parse master address");
    let master_stream = TcpStream::connect(master_addr.clone()).await.expect("Failed to connect to master");

    let mut handler = resp::RespHandler::new(master_stream);

    // Stage 1：sent ping to master
    handler.write_value(Value::Array(vec![Value::BulkString(Some("PING".to_string()))])).await?;
    let response = handler.read_value().await?.ok_or_else(|| anyhow::anyhow!("Failed to read response"))?;
    println!("Master response: {}", response);

    // Stage2: The replica sends twice to the master (This stageREPLCONF)
    {
        let config = redisconfig.lock().await;
        handler.write_value(Value::Array(vec![
            Value::BulkString(Some("REPLCONF".to_string())),
            Value::BulkString(Some("listening-port".to_string())),
            Value::BulkString(Some(config.get_config("port".to_string()).to_string())),
        ])).await?;
    }
    let response = handler.read_value().await?.ok_or_else(|| anyhow::anyhow!("Failed to read response"))?;
    println!("Master response: {}", response);

    handler.write_value(Value::Array(vec![
        Value::BulkString(Some("REPLCONF".to_string())),
        Value::BulkString(Some("capa".to_string())),
        Value::BulkString(Some("psync2".to_string().to_string())),
    ])).await?;

    let response = handler.read_value().await?.ok_or_else(|| anyhow::anyhow!("Failed to read response"))?;
    println!("Master response: {}", response);

    // Stage3: sent PSYNC cmd to master
    // 1.The first argument is the replication ID of the master
    //      Since this is the first time the replica is connecting to the master, the replication ID will be (a question mark)?
    // 2.The second argument is the offset of the master
    //      Since this is the first time the replica is connecting to the master, the offset will be -1

    // TODO: code needs to be refactored
    handler.write_value(Value::Array(vec![
        Value::BulkString(Some("PSYNC".to_string())),
        Value::BulkString(Some("?".to_string())),
        Value::BulkString(Some("-1".to_string().to_string())),
    ])).await?;
    let response = handler.read_value().await?.ok_or_else(|| anyhow::anyhow!("Failed to read response"))?;
    
    println!("Master response: {}", response);

    //读取主机送来的信息
    let response = handler.read_value().await?.ok_or_else(|| anyhow::anyhow!("Failed to read response"))?;
    //TODO: realize redis database storage
    println!("Master response: {:?},{:?}", response,handler);

    //TODO: realize command execution
    tokio::spawn(async move {
        loop {
            let value = handler.read_value().await.unwrap();
            println!("Got value {:?}", value);
    
            // 提前声明变量
            let (command, args): (String, Vec<Value>);
            
            let response = if let Some(v) = value {
                let extracted = extract_command(v).unwrap();
                command = extracted.0; // 初始化变量
                args = extracted.1; // 初始化变量
                
                let respon = db.handle_command(command.clone(), args.clone(), redisconfig.clone(),master_addr.clone()).await;
                println!("{:?}", respon);
                respon
            } else {
                break;
            };
        };
        // TODO：Track command offset 
    });

    Ok(())
}