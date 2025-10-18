use std::time::Duration;

#[allow(dead_code)]
pub struct ParsedResponse {
    pub command: Command,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub ttl: Option<Duration>,
    pub len: u32
}

#[allow(dead_code)]
pub enum Command {
    Set,
    Get,
    Rm,
    IncrementFrequency,
    GetMetadata,
    Ping,
    Size,
    Flush,
    Invalid
}

impl From<String> for Command {
    fn from(value: String) -> Self {
        match value.to_lowercase().as_str() {
            "set" => Self::Set,
            "get" => Self::Get,
            "rm" => Self::Rm,
            "increment_frequency" => Self::IncrementFrequency,
            "get_metadata" => Self::GetMetadata,
            "ping" => Self::Ping,
            "size" => Self::Size,
            "flush" => Self::Flush,
            _ => Self::Invalid
        }
    }
}

impl From<Command> for String {
    fn from(value: Command) -> Self {
        match value {
            Command::Set => "set".to_string(),
            Command::Rm => "rm".to_string(),
            Command::Get => "get".to_string(),
            Command::Ping => "ping".to_string(),
            Command::Size => "size".to_string(),
            Command::Flush => "flush".to_string(),
            Command::GetMetadata => "get_metadata".to_string(),
            Command::IncrementFrequency => "increment_frequency".to_string(),
            Command::Invalid => "Invalid".to_string()
        }
    }
}
