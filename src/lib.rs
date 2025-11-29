//! # EpochDB 🦀
//!
//! An intelligent, persistent, and concurrent key-value store for Rust,
//! designed to manage data with a lifecycle.
//!
//! **EpochDB** is an opinionated database engine built on the robust foundation
//! of `sled`. It's designed specifically for workloads where the relevance of
//! data changes over time, such as caching, session management, and real-time
//! analytics.
//!
//! It provides a high-level, ergonomic API by treating data's **access
//! frequency** and **age** as first-class citizens.

use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::JoinHandle;

use db::errors::TransientError;
use serde::{
    Deserialize,
    Serialize
};
use sled::Tree;

pub mod db;
pub mod metadata;
pub mod metrics;
pub mod server;

/// This is the main struct which represents the database.
///
/// This struct holds the connection to the `sled` database and provides
/// safe, high-level access to the various data trees. It manages a background
/// thread for handling TTL (Time-To-Live) expirations automatically.
///
/// When this struct is dropped, it will signal the background thread to shut
/// down and wait for it to finish gracefully.
///
/// This struct also holds 2 Arc<sled::Tree> directly instead of a single
/// sled::Db, since almost all of the functions uses the tree directly which
/// requires the sled::Db to constantly open each trees.
/// Passing trees from the struct deletes the constant need to open the trees
#[derive(Debug)]
pub struct DB {
    /// Stores the key and value
    data_tree: Arc<Tree>,
    /// Stores the key and the metadata
    meta_tree: Arc<Tree>,
    /// Stores the ttl timestamp and the key
    ttl_tree: Arc<Tree>,
    /// Manage the background thread which checks for expired keys
    ttl_thread: Option<JoinHandle<Result<(), TransientError>>>,
    /// Manage the background thread to periodically check the DB Size
    size_thread: Option<JoinHandle<Result<(), TransientError>>>,
    /// Signals all threads to gracefully shutdown, when the DB is dropped
    shutdown: Arc<AtomicBool>,
    /// Path to the database
    pub path: PathBuf
}

/// Contains additional information about a key, such as its access frequency
/// and lifecycle.
///
/// NOTE: This struct derives Serialize and Deserialize to be stored as raw
/// bytes (&[u8]) in the underlying sled tree.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Metadata {
    /// The number of time the key has been accessed
    pub freq: u64,
    /// Timestamp of key creation, in seconds since the UNIX epoch
    pub created_at: u64,
    /// The key's time-to-live in seconds. If None, the key is persistent and
    /// never expires.
    pub ttl: Option<u64>
}
