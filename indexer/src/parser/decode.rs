use std::collections::HashMap;

use serde_json::Value;

use super::ParseError;
use super::idl::{IdlType, IdlTypeDef};

pub(super) fn decode_type_def(
    def: &IdlTypeDef,
    cursor: &mut Cursor<'_>,
    types: &HashMap<String, IdlTypeDef>,
) -> Result<Value, ParseError> {
    match def {
        IdlTypeDef::Struct { fields, .. } => {
            let mut map = serde_json::Map::new();
            for field in fields {
                let value = decode_idl_type(&field.ty, cursor, types)?;
                map.insert(field.name.clone(), value);
            }
            Ok(Value::Object(map))
        }
        IdlTypeDef::Enum { variants } => {
            let variant_index = cursor.read_u8()?;
            let variant = variants
                .get(variant_index as usize)
                .ok_or_else(|| ParseError::new("enum variant index out of range".to_string()))?;
            let mut map = serde_json::Map::new();
            map.insert("variant".to_string(), Value::String(variant.name.clone()));
            if !variant.fields.is_empty() {
                let mut fields_map = serde_json::Map::new();
                for field in &variant.fields {
                    let value = decode_idl_type(&field.ty, cursor, types)?;
                    fields_map.insert(field.name.clone(), value);
                }
                map.insert("fields".to_string(), Value::Object(fields_map));
            }
            Ok(Value::Object(map))
        }
    }
}

pub(super) fn decode_idl_type(
    ty: &IdlType,
    cursor: &mut Cursor<'_>,
    types: &HashMap<String, IdlTypeDef>,
) -> Result<Value, ParseError> {
    match ty {
        IdlType::Bool => Ok(Value::Bool(cursor.read_u8()? != 0)),
        IdlType::U8 => Ok(Value::String(cursor.read_u8()?.to_string())),
        IdlType::I8 => Ok(Value::String((cursor.read_u8()? as i8).to_string())),
        IdlType::U16 => Ok(Value::String(cursor.read_u16()?.to_string())),
        IdlType::I16 => Ok(Value::String(cursor.read_i16()?.to_string())),
        IdlType::U32 => Ok(Value::String(cursor.read_u32()?.to_string())),
        IdlType::I32 => Ok(Value::String(cursor.read_i32()?.to_string())),
        IdlType::U64 => Ok(Value::String(cursor.read_u64()?.to_string())),
        IdlType::I64 => Ok(Value::String(cursor.read_i64()?.to_string())),
        IdlType::U128 => Ok(Value::String(cursor.read_u128()?.to_string())),
        IdlType::I128 => Ok(Value::String(cursor.read_i128()?.to_string())),
        IdlType::Usize => Ok(Value::String(cursor.read_u64()?.to_string())),
        IdlType::Bytes => {
            let bytes = cursor.read_vec()?;
            Ok(Value::Array(
                bytes.into_iter().map(|b| Value::from(b)).collect(),
            ))
        }
        IdlType::String => {
            let bytes = cursor.read_vec()?;
            match String::from_utf8(bytes) {
                Ok(value) => Ok(Value::String(value)),
                Err(err) => Ok(Value::String(format!("invalid_utf8:{err}"))),
            }
        }
        IdlType::PublicKey => {
            let bytes = cursor.read_fixed(32)?;
            Ok(Value::String(bs58::encode(bytes).into_string()))
        }
        IdlType::Vec(inner) => {
            let len = cursor.read_u32()? as usize;
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                values.push(decode_idl_type(inner, cursor, types)?);
            }
            Ok(Value::Array(values))
        }
        IdlType::Option(inner) => {
            let flag = cursor.read_u8()?;
            if flag == 0 {
                Ok(Value::Null)
            } else {
                decode_idl_type(inner, cursor, types)
            }
        }
        IdlType::Array(inner, len) => {
            let mut values = Vec::with_capacity(*len);
            for _ in 0..*len {
                values.push(decode_idl_type(inner, cursor, types)?);
            }
            Ok(Value::Array(values))
        }
        IdlType::Defined(name) => {
            let def = types
                .get(name)
                .ok_or_else(|| ParseError::new(format!("unknown defined type {name}")))?;
            decode_type_def(def, cursor, types)
        }
    }
}

#[derive(Clone)]
pub(super) struct Cursor<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    pub(super) fn read_fixed(&mut self, len: usize) -> Result<&'a [u8], ParseError> {
        if self.offset + len > self.data.len() {
            return Err(ParseError::new("unexpected end of data".to_string()));
        }
        let slice = &self.data[self.offset..self.offset + len];
        self.offset += len;
        Ok(slice)
    }

    pub(super) fn read_u8(&mut self) -> Result<u8, ParseError> {
        Ok(self.read_fixed(1)?[0])
    }

    pub(super) fn read_u16(&mut self) -> Result<u16, ParseError> {
        let bytes = self.read_fixed(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub(super) fn read_i16(&mut self) -> Result<i16, ParseError> {
        let bytes = self.read_fixed(2)?;
        Ok(i16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub(super) fn read_u32(&mut self) -> Result<u32, ParseError> {
        let bytes = self.read_fixed(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(super) fn read_i32(&mut self) -> Result<i32, ParseError> {
        let bytes = self.read_fixed(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(super) fn read_u64(&mut self) -> Result<u64, ParseError> {
        let bytes = self.read_fixed(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub(super) fn read_i64(&mut self) -> Result<i64, ParseError> {
        let bytes = self.read_fixed(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub(super) fn read_u128(&mut self) -> Result<u128, ParseError> {
        let bytes = self.read_fixed(16)?;
        Ok(u128::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]))
    }

    pub(super) fn read_i128(&mut self) -> Result<i128, ParseError> {
        let bytes = self.read_fixed(16)?;
        Ok(i128::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]))
    }

    pub(super) fn read_vec(&mut self) -> Result<Vec<u8>, ParseError> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_fixed(len)?;
        Ok(bytes.to_vec())
    }

    pub(super) fn remaining(&self) -> Option<&'a [u8]> {
        if self.offset >= self.data.len() {
            None
        } else {
            Some(&self.data[self.offset..])
        }
    }
}
