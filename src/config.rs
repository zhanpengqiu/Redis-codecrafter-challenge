use std::collections::HashMap;
use std::thread;
use crate::resp::Value;
use crate::duplication::RCliInfo;
use crate::slave_stream::Slaves;
use std::time::{Duration, SystemTime};
use regex::Regex;
use crate::resp::RespHandler;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use std::sync::Arc;
use byteorder::{LittleEndian, ReadBytesExt};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom,Cursor};
use std::error::Error;
use std::time::UNIX_EPOCH;
use tokio::time;

#[derive(Debug)]
#[warn(unused_variables)]
pub struct Config{
    rdbfile: HashMap<String, Value>,
    rdbfile_content: HashMap<String, Value>,
    metadata: HashMap<String, Value>,
    expirations: HashMap<String, SystemTime>,
    rcliinfo:RCliInfo,
    slaves_handler:Arc<RwLock<Slaves>>,
}
impl Config {
    pub fn new() -> Self {
        Config {
            rdbfile: HashMap::new(),
            rdbfile_content: HashMap::new(),
            metadata: HashMap::new(),
            expirations:HashMap::new(),
            rcliinfo: RCliInfo::new(),
            slaves_handler: Arc::new(RwLock::new(Slaves::new())),//需要异步处理
        }
    }
    pub async fn slave_loop(&mut self){
        let slaves_clone = self.slaves_handler.clone();
        tokio::spawn(async move {
            loop{
                {
                    //限定锁的作用域
                    let mut slaves_read = slaves_clone.write().await;
                    slaves_read.r#loop().await;
                }
                time::sleep(time::Duration::from_nanos(1)).await;
            }
        });
    }
    pub async fn rcliinfo_track_cmd(&mut self, cmd:Value){
        let mut slaves_write = self.slaves_handler.write().await;
        let _ = slaves_write.get_new_client_cmd(cmd).await;
    }
    pub async fn new_slave_come(&mut self,syn_addr:String,listen_addr:String){
        //插入一个握手信息到slave里面
        let mut slaves_write = self.slaves_handler.write().await;
        let _ = slaves_write.shake_hand_addr_info(syn_addr,listen_addr).await;
    }
    pub async fn add_slave_resphandler(&mut self,handler:RespHandler){
        //在slave信息里面实现连接
        let mut slaves_write = self.slaves_handler.write().await;
        slaves_write.add_new_slave_handler(handler).await;
    }
    pub fn set_rcliinfo(&mut self,key:String,value:String){
        match key.as_str(){
            "role" => self.rcliinfo.set_role(value),
            _ => {
                println!("Unknown config key: {}", key);
            },
        }
    }
    pub fn insert(&mut self, name: String, value: String){
        self.rdbfile.insert(name, Value::BulkString(Some(value)));
    }
    pub fn get_config(&self, key:String)->String{
        match self.rdbfile.get(&key) {
            Some(value) => {
                match value {
                    Value::BulkString(Some(s)) => s.clone(),
                    _ => "Unknown".to_string(),
                }
            },
            None => "Unknown".to_string(),
        }
    }
    pub fn load_rdb(&mut self) {
        let dir_name = "dir".to_string();
        let dbfile_name = "dbfilename".to_string();

        // 获取目录名，默认值为 "./"
        let path = match self.rdbfile.get(&dir_name) {
            Some(Value::BulkString(Some(ref s))) => s.clone(),
            _ => "./".to_string(),
        };

        // 获取文件名，默认值为 "dump.rdb"
        let file_name = match self.rdbfile.get(&dbfile_name) {
            Some(Value::BulkString(Some(ref s))) => s.clone(),
            _ => "dump.rdb".to_string(),
        };

        // 组合路径
        let full_path = format!("{}/{}", path, file_name);
        // 调用加载文件的方法
        let _ = self.load_from_file(&full_path);
    }
    pub fn config_get(&self, key: String) -> Value {
        match self.rdbfile.get(&key) {
            Some(value) => {
                let valuearray=Value::Array(vec![
                    Value::BulkString(Some(key.clone())),
                    value.clone(),
                ]);
                valuearray
            },
            None => Value::BulkString(None),
        }
    }
    pub fn get(&mut self, key: String) -> Value{
        // Check if the key has expired
        println!("{:?},{:?}",self.expirations,SystemTime::now());
        if let Some(expiration_time) = self.expirations.get(&key) {
            if let Ok(now) = SystemTime::now().duration_since(*expiration_time) {
                if now > Duration::from_secs(0) {
                    self.rdbfile_content.remove(&key);
                    self.expirations.remove(&key);
                    return Value::BulkString(None);
                }
                println!("{:?},{:?}",now,Duration::from_secs(0));
            }
        }
        match self.rdbfile_content.get(&key) {
            Some(value) => value.clone(),
            None => Value::BulkString(None),
        }
        // self.rdbfile_content.get(key)

    }
    pub fn set(&mut self, key: String, value: Value) -> Value {
        self.rdbfile_content.insert(key, value);
        Value::SimpleString("OK".to_string())
    }
    pub fn set_expriations(&mut self,key:String,expiration_time:SystemTime){
        self.expirations.insert(key,expiration_time);
    }
    pub fn get_keys(&self, pattern: String) ->  Value{
        let _regex = Self::pattern_to_regex(&pattern);
        if pattern == "Cargo.lock".to_string(){
            //返回所有的key
            Value::Array(
                self.rdbfile_content
                    .keys()
                    .map(|k| Value::BulkString(Some(k.clone())))
                    .collect(),
            )
        }
        else if pattern.contains('*') || pattern.contains('?') {
            let regex = Self::pattern_to_regex(&pattern);
            let keys_pattern_iter = self.rdbfile_content
                .iter()
                .filter(|(k, _)| regex.is_match(k))
                .map(|(k, _)| k.clone())
                .collect::<Vec<String>>();
                Value::Array(keys_pattern_iter.into_iter().map(|k| Value::BulkString(Some(k))).collect())
        } else {
            // 查找具体的键
            match self.rdbfile_content.get(&pattern) {
                Some(_) => Value::BulkString(Some(pattern.clone())),
                None => Value::Array(vec![]), // 如果键不存在，返回空数组
            }
            
        }
    }
    pub fn get_info_replication(&self)->Value{
        Value::BulkString(Some(self.rcliinfo.get_replication_info()))
    }

    pub fn get_key_info_of_replication(&self, key:String)->Value{
        self.rcliinfo.get_param(key)
    }

    fn pattern_to_regex(pattern: &str) -> Regex {
        let escaped = regex::escape(pattern);
        let pattern = escaped.replace("\\*", ".*").replace("\\?", ".");
        Regex::new(&pattern).unwrap_or_else(|_| Regex::new(".*").unwrap())
    }

    pub fn load_from_file(&mut self, path: &str) -> io::Result<()> {
        println!("{}",path);
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut cursor = Cursor::new(buffer);
        self.parse_rdb(&mut cursor)
    }

    fn parse_rdb(&mut self, cursor: &mut Cursor<Vec<u8>>) -> io::Result<()> {
        // Check magic string and version
        let mut magic = [0u8; 5];
        cursor.read_exact(&mut magic)?;
    
        let mut version = [0u8; 4];
        cursor.read_exact(&mut version)?;
        println!("{:?},{:?}",magic,version);
    
        while cursor.position() < cursor.get_ref().len() as u64 - 8 {
            match cursor.read_u8()? {
                0xFA => self.parse_aux_field(cursor)?,
                0xFE => self.parse_db_selector(cursor)?,
                0xFF => break, // End of RDB file
                value_type => self.parse_key_value_pair(value_type, cursor)?,
            }
        }
    
        // Verify checksum
        let checksum = cursor.read_u64::<LittleEndian>()?;
        // You need to calculate the expected checksum and compare it with `checksum`
        // For simplicity, we assume the checksum is correct here
        println!("Checksum verified: {}", checksum);
    
        Ok(())
    }

    fn parse_aux_field(&mut self, cursor: &mut Cursor<Vec<u8>>) -> io::Result<()> {
        // Parse auxiliary field
        let key = self.parse_string(cursor)?;
        println!("{:?}",key);
        let value = self.parse_value(cursor)?;
        println!("{:?}",value);
        self.metadata.insert(key, value);

        Ok(())
    }

    fn parse_db_selector(&mut self, cursor: &mut Cursor<Vec<u8>>) -> io::Result<()> {
        //第一个字节是选择数据库标记的
        let db_index = cursor.read_u8()?;
        println!("Switching to database index: {}", db_index);

        //然后读取当前字符是不是FB，是的话进行处理数据库内容
        let current_position = cursor.position();
        match cursor.read_u8()?{
            0xFB => {
                //读取数据库大小
                let db_hash_table_size = cursor.read_u8()?;
                match cursor.read_u8()? {
                    0x00 =>{
                        // no expire time
                        // 读取第一个字符是决定你当选的内容是什么格式的，目前只实现了00-》字符串
                        for _i in 0..db_hash_table_size{
                            match cursor.read_u8()?{
                                0x00 =>{
                                    let key = self.parse_string(cursor)?;
                                    let value = self.parse_string(cursor)?;
                                    self.rdbfile_content.insert(key.clone(),Value::BulkString(Some(value.clone())));
                                    println!("Resizedb field: num_keys={:?}, num_expires={:?}", key, value);
                                }
                                _ => println!("Falied1"),
                            }
                        }

                    }
                    _ =>{
                        //读取expire time
                        //判断第一个字节是不是fc，不是就正常获取数据
                        for _i in 0..db_hash_table_size{
                            let tmp_position = cursor.position();
                            match cursor.read_u8()?{
                                0xFC => {
                                    // 获取expire time
                                    let expiry_time = cursor.read_u64::<LittleEndian>()?;
                                    println!("Expiry time in milliseconds: {}", expiry_time);
                                    match cursor.read_u8()?{
                                        0x00 =>{
                                            //判断是不是过期了，没有过期就跳过
                                            let current_time = SystemTime::now()
                                                                    .duration_since(UNIX_EPOCH)
                                                                    .expect("Time went backwards")
                                                                    .as_millis() as u64;
                                            println!("{:?}",current_time);
                                            let key = self.parse_string(cursor)?;
                                            let value = self.parse_string(cursor)?;
                                            if current_time< expiry_time{
                                                self.rdbfile_content.insert(key.clone(),Value::BulkString(Some(value.clone())));

                                                let time:SystemTime = UNIX_EPOCH + Duration::from_millis(expiry_time);
                                                self.expirations.insert(key.clone(), time);
                                                println!("Resizedb field: num_keys={:?}, num_expires={:?}", key, value);
                                            }                                            
                                        }
                                        _ => println!("Falied2"),
                                    }
                                }
                                0xFD => {
                                    // 获取expire time
                                    let expiry_time = cursor.read_u32::<LittleEndian>()?;
                                    println!("Expiry time in milliseconds: {}", expiry_time);
                                    match cursor.read_u8()?{
                                        0x00 =>{
                                            //判断是不是过期了，没有过期就跳过
                                            let current_time = SystemTime::now()
                                                                    .duration_since(UNIX_EPOCH)
                                                                    .expect("Time went backwards")
                                                                    .as_secs() as u32;
                                            let key = self.parse_string(cursor)?;
                                            let value = self.parse_string(cursor)?;
                                            if current_time< expiry_time{
                                                self.rdbfile_content.insert(key.clone(),Value::BulkString(Some(value.clone())));
                                                let time:SystemTime = UNIX_EPOCH + Duration::from_secs(expiry_time.into());
                                                self.expirations.insert(key.clone(), time);
                                                println!("Resizedb field: num_keys={:?}, num_expires={:?}", key, value);
                                            }                                            
                                        }
                                        _ => println!("Falied2"),
                                    }
                                }
                                _ => {
                                    cursor.set_position(tmp_position);
                                    match cursor.read_u8()?{
                                        0x00 =>{
                                            let key = self.parse_string(cursor)?;
                                            let value = self.parse_string(cursor)?;
                                            self.rdbfile_content.insert(key.clone(),Value::BulkString(Some(value.clone())));
                                            println!("Resizedb field: num_keys={:?}, num_expires={:?}", key, value);
                                        }
                                        _ => println!("Falied3"),
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _=>{
                cursor.set_position(current_position);
            }
        };
        Ok(())
    }

    fn parse_resizedb_field(&mut self, cursor: &mut Cursor<Vec<u8>>) -> io::Result<()> {
        // Parse resizedb field
        let num_of_items = cursor.read_u16::<LittleEndian>()?;
        for _i in 0..num_of_items {
            let encoding_type = cursor.read_u8()?;
            match encoding_type{
                0x00=> {
                    let key = self.parse_string(cursor)?;
                    let value = self.parse_string(cursor)?;
                    self.rdbfile_content.insert(key.clone(),Value::BulkString(Some(value.clone())));
                    println!("Resizedb field: num_keys={:?}, num_expires={:?}", key, value);
                }
                _ => println!("Falied"),
            }
        }
        
        Ok(())
    }

    fn parse_expiry_time_seconds(&mut self, cursor: &mut Cursor<Vec<u8>>) -> io::Result<()> {
        // Parse expiry time in seconds
        let expiry_time = cursor.read_u32::<LittleEndian>()?;
        println!("Expiry time in seconds: {}", expiry_time);
        Ok(())
    }

    fn parse_expiry_time_milliseconds(&mut self, cursor: &mut Cursor<Vec<u8>>) -> io::Result<()> {
        // Parse expiry time in milliseconds
        let expiry_time = cursor.read_u64::<LittleEndian>()?;
        println!("Expiry time in milliseconds: {}", expiry_time);
        Ok(())
    }

    fn parse_key_value_pair(&mut self, _value_type: u8, cursor: &mut Cursor<Vec<u8>>) -> io::Result<()> {
        // Parse key-value pair
        let key = self.parse_string(cursor)?;
        let value = self.parse_value(cursor)?;
        self.rdbfile_content.insert(key, value);
        Ok(())
    }

    fn parse_string(&mut self, cursor: &mut Cursor<Vec<u8>>) -> io::Result<String> {
        let len = cursor.read_u8()? as usize;
        let mut buf = vec![0; len];
        cursor.read_exact(&mut buf)?;
        String::from_utf8(buf).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8"))
    }

    fn parse_value(&mut self, cursor: &mut Cursor<Vec<u8>>) -> io::Result<Value> {
        let value_type = cursor.read_u8()?;
        match value_type {
            0 => Ok(Value::BulkString(None)),
            1..=0xBF => {
                let len = value_type as usize;
                let mut buf = vec![0; len];
                cursor.read_exact(&mut buf)?;
                let value_str = String::from_utf8(buf).unwrap();
                Ok(Value::SimpleString(value_str))
            }
            0xC0..=0xC3 => {
                let len = value_type - 0xC0;
                let value_int = match len {
                    0 => cursor.read_u8()? as i64,
                    1 => cursor.read_u16::<LittleEndian>()? as i64,
                    2 => cursor.read_u32::<LittleEndian>()? as i64,
                    3 => cursor.read_i64::<LittleEndian>()?,
                    _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid value length")),
                };
                Ok(Value::Integer(value_int))
            }
            0xC4..=0xC7 => {
                // Handle other types like List, Set, Hash, etc.
                unimplemented!()
            }
            0xFF => Ok(Value::BulkString(None)), // End of value
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown value type")),
        }
    }
}

