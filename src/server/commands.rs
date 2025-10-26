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

impl From<Command> for &[u8] {
    fn from(value: Command) -> Self {
        match value {
            Command::Set => b"set",
            Command::Rm => b"rm",
            Command::Get => b"get",
            Command::Ping => b"ping",
            Command::Size => b"size",
            Command::Flush => b"flush",
            Command::GetMetadata => b"get_metadata",
            Command::IncrementFrequency => b"increment_frequency",
            Command::Invalid => b"Invalid"
        }
    }
}

impl From<&[u8]> for Command {
    fn from(value: &[u8]) -> Self {
        if value.eq_ignore_ascii_case(b"set") {
            Command::Set
        }
        else if value.eq_ignore_ascii_case(b"rm") {
            Command::Rm
        }
        else if value.eq_ignore_ascii_case(b"get") {
            Command::Get
        }
        else if value.eq_ignore_ascii_case(b"ping") {
            Command::Ping
        }
        else if value.eq_ignore_ascii_case(b"size") {
            Command::Size
        }
        else if value.eq_ignore_ascii_case(b"flush") {
            Command::Flush
        }
        else if value.eq_ignore_ascii_case(b"get_metadata") {
            Command::GetMetadata
        }
        else if value.eq_ignore_ascii_case(b"increment_frequency") {
            Command::IncrementFrequency
        }
        else {
            // If the command is not recognized
            Command::Invalid
        }
    }
}





























