use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::resp::Value;
use std::time::{Duration, SystemTime};
use regex::Regex;

use byteorder::{LittleEndian, ReadBytesExt};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom,Cursor};
use std::error::Error;

#[derive(Debug)]
struct DataObject {
    key: String,
    value: Value,
}

#[derive(Debug)]
pub struct Config{
    rdbfile: HashMap<String, Value>,
    rdbfile_content: HashMap<String, Value>,
    metadata: HashMap<String, Value>,
    expirations: HashMap<String, SystemTime>,

}
impl Config {
    pub fn new() -> Self {
        Config {
            rdbfile: HashMap::new(),
            rdbfile_content: HashMap::new(),
            metadata: HashMap::new(),
            expirations:HashMap::new(),
        }
    }
    pub fn insert(&mut self, name: String, value: String){
        self.rdbfile.insert(name, Value::BulkString(Some(value)));
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
        self.load_from_file(&full_path);
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
        if let Some(expiration_time) = self.expirations.get(&key) {
            if let Ok(now) = SystemTime::now().duration_since(*expiration_time) {
                if now > Duration::from_secs(0) {
                    self.rdbfile_content.remove(&key);
                    self.expirations.remove(&key);
                    return Value::BulkString(None);
                }
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
        let regex = Self::pattern_to_regex(&pattern);
        
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
                0xFB => self.parse_resizedb_field(cursor)?,
                0xFD => self.parse_expiry_time_seconds(cursor)?,
                0xFC => self.parse_expiry_time_milliseconds(cursor)?,
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
        // Parse database selector
        let db_index = cursor.read_u8()?;
        println!("Switching to database index: {}", db_index);
        Ok(())
    }

    fn parse_resizedb_field(&mut self, cursor: &mut Cursor<Vec<u8>>) -> io::Result<()> {
        // Parse resizedb field
        let num_of_items = cursor.read_u16::<LittleEndian>()?;
        for i in 0..num_of_items {
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

    fn parse_key_value_pair(&mut self, value_type: u8, cursor: &mut Cursor<Vec<u8>>) -> io::Result<()> {
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

