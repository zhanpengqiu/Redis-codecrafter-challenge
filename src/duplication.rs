use std::collections::HashMap;
use crate::resp::Value;
use anyhow::Result;
use tokio::sync::Mutex;
use std::sync::Arc;
#[derive(Debug)]
pub struct RCliInfo {
    replication_info: HashMap<String, Value>,
}

impl RCliInfo {
    /// 初始化 RCliInfo 结构体并填充复制信息
    pub fn new() -> Self {
        let mut replication_info = HashMap::new();

        replication_info.insert("role".to_string(), Value::SimpleString("master".to_string()));
        replication_info.insert("connected_slaves".to_string(), Value::Integer(0));
        replication_info.insert("master_replid".to_string(), Value::SimpleString("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()));
        replication_info.insert("master_repl_offset".to_string(), Value::Integer(0));
        replication_info.insert("second_repl_offset".to_string(), Value::Integer(-1));
        replication_info.insert("repl_backlog_active".to_string(), Value::SimpleString("0".to_string()));
        replication_info.insert("repl_backlog_size".to_string(), Value::Integer(1048576));
        replication_info.insert("repl_backlog_first_byte_offset".to_string(), Value::Integer(0));
        replication_info.insert("repl_backlog_histlen".to_string(), Value::Integer(0));

        RCliInfo { replication_info}
    }

    /// 获取复制信息，返回一个符合 Redis 协议的批量字符串
    pub fn get_replication_info(&self) -> String {
        let mut response = String::new();
        let keys_in_order = vec![
            "role",
            "connected_slaves",
            "master_replid",
            "master_repl_offset",
            "second_repl_offset",
            "repl_backlog_active",
            "repl_backlog_size",
            "repl_backlog_first_byte_offset",
            "repl_backlog_histlen",
        ];

        for key in keys_in_order {
            if let Some(value) = self.replication_info.get(key) {
                response.push_str(&format!("{}:{}\n", key, value));
            }
        }

        response.to_string()
    }
    pub fn set_role(&mut self, role:String){
        self.replication_info.insert("role".to_string(), Value::SimpleString(role));
    }

    pub fn get_param(&self, param:String)->Value{
        match self.replication_info.get(&param) {
            Some(value) => value.clone(),
            None => Value::BulkString(None),
        }
    }

}
