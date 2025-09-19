//! The `metadata` module defines the `Metadata` struct and its associated
//! methods. `Metadata` is used to track information about each key-value
//! pair, such as its creation time, access frequency, and TTL.

use std::time::{
    SystemTime,
    UNIX_EPOCH
};
use std::vec;

use bincode::error::{
    DecodeError,
    EncodeError
};
use bincode::serde::{
    decode_from_slice,
    encode_to_vec
};

use crate::Metadata;

impl Metadata {
    /// Creates a new `Metadata` instance with an optional TTL.
    ///
    /// The `created_at` timestamp is set to the current system time.
    pub fn new(ttl: Option<u64>) -> Metadata {
        let currtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Cant get the current time")
            .as_secs();
        Metadata {
            freq: 0,
            created_at: currtime,
            ttl
        }
    }

    /// Increments the frequency counter.
    pub fn freq_incretement(mut self) -> Metadata {
        self.freq += 1;
        self
    }

    /// Decrements the frequency counter.
    pub fn freq_decretement(mut self) -> Metadata {
        self.freq -= 1;
        self
    }

    /// Serializes the `Metadata` instance into a byte vector using `bincode`.
    ///
    /// # Errors
    ///
    /// Returns an `EncodeError` if serialization fails.
    pub fn to_u8(&self) -> Result<Vec<u8>, EncodeError> {
        encode_to_vec(self, bincode::config::standard())
    }

    /// Deserializes a `Metadata` instance from a byte slice using `bincode`.
    ///
    /// # Errors
    ///
    /// Returns a `DecodeError` if deserialization fails.
    pub fn from_u8(slice: &[u8]) -> Result<Metadata, DecodeError> {
        Ok(decode_from_slice(slice, bincode::config::standard())?.0)
    }

    pub fn to_response(&self) -> Vec<(String, RespValue)> {
        vec![
            ("frequency".to_string(), RespValue::U64(self.freq)),
            ("created_at".to_string(), RespValue::U64(self.created_at)),
            ("ttl".to_string(), 
                match self.ttl  {
                    Some(t) => RespValue::U64(t),
                    None => RespValue::None
                }
            ),
        ]
    }
}

pub enum RespValue {
    U64(u64),
    I32(i32),
    BulkString(Vec<u8>),
    None
}
