use serde_json::Value;

use super::ParseError;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(super) struct Discriminator([u8; 8]);

impl Discriminator {
    pub(super) fn from_slice(bytes: &[u8]) -> Self {
        let mut data = [0u8; 8];
        data.copy_from_slice(bytes);
        Self(data)
    }
    pub(super) fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

#[derive(Debug, Clone)]
pub(super) struct Idl {
    pub(super) address: String,
    pub(super) instructions: Vec<IdlInstruction>,
    pub(super) events: Vec<IdlEvent>,
    pub(super) errors: Vec<IdlError>,
    pub(super) accounts: Vec<IdlAccount>,
    pub(super) types: Vec<IdlTypeEntry>,
}

impl Idl {
    pub(super) fn from_json(raw: &str) -> Result<Self, ParseError> {
        let value: Value = serde_json::from_str(raw)
            .map_err(|err| ParseError::new(format!("invalid idl json: {err}")))?;
        let address = value
            .get("address")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::new("missing idl address".to_string()))?
            .to_string();

        let instructions_value = value
            .get("instructions")
            .and_then(Value::as_array)
            .ok_or_else(|| ParseError::new("missing idl instructions".to_string()))?;
        let mut instructions = Vec::new();
        for instruction in instructions_value {
            instructions.push(IdlInstruction::from_value(instruction)?);
        }

        let mut accounts = Vec::new();
        if let Some(accounts_value) = value.get("accounts").and_then(Value::as_array) {
            for account in accounts_value {
                accounts.push(IdlAccount::from_value(account)?);
            }
        }

        let mut events = Vec::new();
        if let Some(events_value) = value.get("events").and_then(Value::as_array) {
            for event in events_value {
                events.push(IdlEvent::from_value(event)?);
            }
        }

        let mut errors = Vec::new();
        if let Some(errors_value) = value.get("errors").and_then(Value::as_array) {
            for err in errors_value {
                errors.push(IdlError::from_value(err)?);
            }
        }

        let mut types = Vec::new();
        if let Some(types_value) = value.get("types").and_then(Value::as_array) {
            for entry in types_value {
                types.push(IdlTypeEntry::from_value(entry)?);
            }
        }

        Ok(Self {
            address,
            instructions,
            events,
            errors,
            accounts,
            types,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct IdlInstruction {
    pub(super) name: String,
    pub(super) discriminator: Discriminator,
    pub(super) accounts: Vec<IdlAccountItem>,
    pub(super) args: Vec<IdlField>,
}

impl IdlInstruction {
    fn from_value(value: &Value) -> Result<Self, ParseError> {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::new("instruction missing name".to_string()))?
            .to_string();

        let discriminator = parse_discriminator(value.get("discriminator"))
            .ok_or_else(|| ParseError::new(format!("instruction {name} missing discriminator")))?;

        let accounts_value = value
            .get("accounts")
            .and_then(Value::as_array)
            .ok_or_else(|| ParseError::new(format!("instruction {name} missing accounts")))?;
        let mut accounts = Vec::new();
        for account in accounts_value {
            accounts.push(IdlAccountItem::from_value(account)?);
        }

        let mut args = Vec::new();
        if let Some(args_value) = value.get("args").and_then(Value::as_array) {
            for arg in args_value {
                args.push(IdlField::from_value(arg)?);
            }
        }

        Ok(Self {
            name,
            discriminator,
            accounts,
            args,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct IdlEvent {
    pub(super) name: String,
    pub(super) discriminator: Discriminator,
}

impl IdlEvent {
    fn from_value(value: &Value) -> Result<Self, ParseError> {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::new("event missing name".to_string()))?
            .to_string();

        let discriminator = parse_discriminator(value.get("discriminator"))
            .ok_or_else(|| ParseError::new(format!("event {name} missing discriminator")))?;

        Ok(Self {
            name,
            discriminator,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct IdlError {
    pub(super) code: u32,
    pub(super) name: String,
    pub(super) msg: Option<String>,
}

impl IdlError {
    fn from_value(value: &Value) -> Result<Self, ParseError> {
        let code = value
            .get("code")
            .and_then(Value::as_u64)
            .ok_or_else(|| ParseError::new("error missing code".to_string()))
            .and_then(|v| {
                u32::try_from(v).map_err(|_| ParseError::new("error code out of range".to_string()))
            })?;
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::new("error missing name".to_string()))?
            .to_string();
        let msg = value
            .get("msg")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        Ok(Self { code, name, msg })
    }
}

#[derive(Debug, Clone)]
pub(super) struct IdlAccount {
    pub(super) name: String,
    pub(super) discriminator: Discriminator,
}

impl IdlAccount {
    fn from_value(value: &Value) -> Result<Self, ParseError> {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::new("account missing name".to_string()))?
            .to_string();
        let discriminator = parse_discriminator(value.get("discriminator"))
            .ok_or_else(|| ParseError::new(format!("account {name} missing discriminator")))?;
        Ok(Self {
            name,
            discriminator,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct IdlAccountItem {
    pub(super) name: String,
    pub(super) optional: bool,
}

impl IdlAccountItem {
    fn from_value(value: &Value) -> Result<Self, ParseError> {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::new("account item missing name".to_string()))?
            .to_string();
        let optional = value
            .get("optional")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        Ok(Self { name, optional })
    }
}

#[derive(Debug, Clone)]
pub(super) struct IdlField {
    pub(super) name: String,
    pub(super) ty: IdlType,
}

impl IdlField {
    fn from_value(value: &Value) -> Result<Self, ParseError> {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::new("field missing name".to_string()))?
            .to_string();
        let ty_value = value
            .get("type")
            .ok_or_else(|| ParseError::new(format!("field {name} missing type")))?;
        let ty = IdlType::from_value(ty_value)?;
        Ok(Self { name, ty })
    }
}

#[derive(Debug, Clone)]
pub(super) struct IdlTypeEntry {
    pub(super) name: String,
    pub(super) def: IdlTypeDef,
}

impl IdlTypeEntry {
    fn from_value(value: &Value) -> Result<Self, ParseError> {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::new("type entry missing name".to_string()))?
            .to_string();
        let def_value = value
            .get("type")
            .ok_or_else(|| ParseError::new(format!("type {name} missing type")))?;
        let def = IdlTypeDef::from_value(def_value)?;
        Ok(Self { name, def })
    }
}

#[derive(Debug, Clone)]
pub(super) enum IdlTypeDef {
    Struct { fields: Vec<IdlField> },
    Enum { variants: Vec<IdlEnumVariant> },
}

impl IdlTypeDef {
    fn from_value(value: &Value) -> Result<Self, ParseError> {
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::new("type def missing kind".to_string()))?;
        match kind {
            "struct" => {
                let mut fields = Vec::new();
                if let Some(fields_value) = value.get("fields").and_then(Value::as_array) {
                    for field in fields_value {
                        fields.push(IdlField::from_value(field)?);
                    }
                }
                Ok(Self::Struct { fields })
            }
            "enum" => {
                let mut variants = Vec::new();
                if let Some(variants_value) = value.get("variants").and_then(Value::as_array) {
                    for variant in variants_value {
                        variants.push(IdlEnumVariant::from_value(variant)?);
                    }
                }
                Ok(Self::Enum { variants })
            }
            _ => Err(ParseError::new(format!("unsupported kind {kind}"))),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct IdlEnumVariant {
    pub(super) name: String,
    pub(super) fields: Vec<IdlField>,
}

impl IdlEnumVariant {
    fn from_value(value: &Value) -> Result<Self, ParseError> {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| ParseError::new("variant missing name".to_string()))?
            .to_string();
        let mut fields = Vec::new();
        if let Some(fields_value) = value.get("fields").and_then(Value::as_array) {
            for (idx, field) in fields_value.iter().enumerate() {
                if field.is_string() || field.get("type").is_some() {
                    let ty_value = if let Some(ty) = field.get("type") {
                        ty
                    } else {
                        field
                    };
                    let ty = IdlType::from_value(ty_value)?;
                    let generated_name = field
                        .get("name")
                        .and_then(Value::as_str)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| format!("field_{idx}"));
                    fields.push(IdlField {
                        name: generated_name,
                        ty,
                    });
                } else {
                    fields.push(IdlField::from_value(field)?);
                }
            }
        }
        Ok(Self { name, fields })
    }
}

#[derive(Debug, Clone)]
pub(super) enum IdlType {
    Bool,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    U128,
    I128,
    Usize,
    Bytes,
    String,
    PublicKey,
    Vec(Box<IdlType>),
    Option(Box<IdlType>),
    Array(Box<IdlType>, usize),
    Defined(String),
}

impl IdlType {
    fn from_value(value: &Value) -> Result<Self, ParseError> {
        if let Some(s) = value.as_str() {
            return match s {
                "bool" => Ok(Self::Bool),
                "u8" => Ok(Self::U8),
                "i8" => Ok(Self::I8),
                "u16" => Ok(Self::U16),
                "i16" => Ok(Self::I16),
                "u32" => Ok(Self::U32),
                "i32" => Ok(Self::I32),
                "u64" => Ok(Self::U64),
                "i64" => Ok(Self::I64),
                "u128" => Ok(Self::U128),
                "i128" => Ok(Self::I128),
                "usize" => Ok(Self::Usize),
                "bytes" => Ok(Self::Bytes),
                "string" => Ok(Self::String),
                "pubkey" | "publicKey" => Ok(Self::PublicKey),
                other => Err(ParseError::new(format!("unsupported type {other}"))),
            };
        }

        let obj = value
            .as_object()
            .ok_or_else(|| ParseError::new("type must be string or object".to_string()))?;

        if let Some(defined) = obj.get("defined") {
            if let Some(name) = defined.as_str() {
                return Ok(Self::Defined(name.to_string()));
            }
            if let Some(name) = defined.get("name").and_then(Value::as_str) {
                return Ok(Self::Defined(name.to_string()));
            }
        }

        if let Some(vec_value) = obj.get("vec") {
            return Ok(Self::Vec(Box::new(Self::from_value(vec_value)?)));
        }

        if let Some(option_value) = obj.get("option") {
            return Ok(Self::Option(Box::new(Self::from_value(option_value)?)));
        }

        if let Some(array_value) = obj.get("array") {
            let array = array_value
                .as_array()
                .ok_or_else(|| ParseError::new("array type must be array".to_string()))?;
            if array.len() != 2 {
                return Err(ParseError::new(
                    "array type must have two elements".to_string(),
                ));
            }
            let inner = Self::from_value(&array[0])?;
            let len = array[1]
                .as_u64()
                .ok_or_else(|| ParseError::new("array length must be number".to_string()))?;
            let len = usize::try_from(len)
                .map_err(|_| ParseError::new("array length too large".to_string()))?;
            return Ok(Self::Array(Box::new(inner), len));
        }

        Err(ParseError::new("unsupported type object".to_string()))
    }
}

fn parse_discriminator(value: Option<&Value>) -> Option<Discriminator> {
    let list = value?.as_array()?;
    if list.len() != 8 {
        return None;
    }
    let mut data = [0u8; 8];
    for (idx, item) in list.iter().enumerate() {
        let byte = item.as_u64()? as u8;
        data[idx] = byte;
    }
    Some(Discriminator(data))
}
