// 定义 Slaves 结构体
use crate::resp::RespHandler;
use crate::resp::Value;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
use hex;
use std::fs::File;
use std::io::{self, Read};
use std::collections::VecDeque;
use anyhow::Result;
use tokio::time;
#[warn(unused_variables)]
#[derive(Debug)]
pub struct Slaves {
    slave_addrs: HashMap<String,String>,
    slave_handler:Vec<RespHandler>,
    slave_offsets:Vec<i64>,

    command_hash: Vec<Value>,
    slave_command_hash_index:Vec<i64>,
}

impl Slaves {
    pub fn new() -> Self {
        Slaves {
            slave_addrs: HashMap::new(),
            slave_handler: Vec::new(),
            slave_offsets: Vec::new(),

            command_hash: Vec::new(),
            slave_command_hash_index: Vec::new(),
        }
    }
    //用于循环发送心跳包给slave
    pub async fn r#loop(&mut self) {
        // println!("{:?},{:?},{:?}",self.command_hash,self.slave_command_hash_index,self.command_hash.len());
        for (index, item) in self.slave_command_hash_index.iter_mut().enumerate() {
            if *item<self.command_hash.len() as i64{
                for (command_index,command) in self.command_hash.iter().enumerate().skip(*item as usize) {
                    // self.slave_handler
                    // 写入数据
                    if let Some(handler) = self.slave_handler.get_mut(index) {
                        println!("{:?}",command);
                        handler.write_value(command.clone()).await;
                    } else {
                        println!("Index {} is out of bounds", index);
                    }
                    *item+=1;
                    break;
                }
            }else{
                //发送ack命令
                for handler in self.slave_handler.iter_mut(){
                    let mut getack_cmd_vec = Vec::new();
                    getack_cmd_vec.push(Value::BulkString(Some("REPLCONF".to_string())));
                    getack_cmd_vec.push(Value::BulkString(Some("getack".to_string())));
                    getack_cmd_vec.push(Value::BulkString(Some("*".to_string())));

                    handler.write_value(Value::Array(getack_cmd_vec)).await;
                }
            }
        }
    }

    pub async fn shake_hand_addr_info(&mut self, in_addr: String,listen_addr: String) {
        println!("New ShakeHand connection came");
        self.slave_addrs.insert(in_addr,listen_addr);
    }

    pub async fn add_new_slave_handler(&mut self,mut handler:RespHandler){
        //写入数据进去
        let val=Slaves::get_empty_rdbfile();
        time::sleep(time::Duration::from_millis(50)).await;
        handler.write_value(val).await.unwrap();
        
        self.slave_handler.push(handler);
        self.slave_offsets.push(0);

        self.slave_command_hash_index.push(0);
    }
    pub fn get_empty_rdbfile()->Value{
        // 打开文件
        let empty_file_payload = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").unwrap();
        // 创建一个缓冲区
        let mut buffer = Vec::new();
        // 将解码后的字节数据放入缓冲区
        buffer.extend_from_slice(&empty_file_payload);

        Value::RdbFile(buffer)
    }
    pub async fn get_new_client_cmd(&mut self, cmd:Value)->Result<String>{
        let command_string = match cmd.clone() {
            Value::Array(a) => {
                   let cmd_clone=a.first().unwrap().clone();
                   match cmd_clone {
                       Value::BulkString(Some(s)) => s.clone(),
                       _ => return Err(anyhow::anyhow!("Unexpected command format")),
                   }
            },
            _ => return Err(anyhow::anyhow!("Unexpected command format")),
        };
        match command_string.to_lowercase().as_str(){
            "set"|"del" =>{
                self.command_hash.push(cmd.clone());
            }
            _ => {

            }
        }
        //TODO: realize command logic
        Ok("Yes, command".to_string())
    }
}