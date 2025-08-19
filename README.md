# EpochDB 🦀

<p align="center">
  <img src="https://github.com/TheZoq2/ferris/blob/master/animated/output/wave.gif?raw=true" width="200" alt="Ferris the crab gif :)">
  <br>
  An intelligent, persistent, and concurrent key-value store for Rust, designed to manage data with a lifecycle.
</p>

<p align="center">
  <a href="https://github.com/FabioCanavarro/EpochDB/actions/workflows/Validation.yml"><img src="https://github.com/FabioCanavarro/EpochDB/actions/workflows/Validation.yml/badge.svg" alt="CI Status"></a>
  <a href="https://crates.io/crates/epoch-db"><img src="https://img.shields.io/crates/v/epoch-db.svg" alt="Crates.io"></a>
  <a href="https://docs.rs/epoch-db"><img src="https://docs.rs/epoch-db/badge.svg" alt="Docs.rs"></a>
  <a href="https://github.com/FabioCanavarro/EpochDB/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg" alt="License"></a>
</p>

---

**EpochDB** is not just another key-value store. It's an opinionated database engine built on the robust foundation of [`sled`](https://github.com/spacejam/sled), designed specifically for workloads where the relevance of data changes over time.

It provides a high-level, ergonomic API to solve common problems like caching, session management, and real-time analytics by treating data's **access frequency** and **age** as first-class citizens.

## 🤔 Why EpochDB?

Many applications need to handle data that isn't meant to live forever. Think of user sessions, cached API responses, or event streams. Managing this "transient" data can be complex. You need to worry about:

* **Performance:** How do you track usage without slowing down your main application?
* **Storage:** How do you prevent old, unused data from filling up your disk?
* **Concurrency:** How do you handle everything safely in a multi-threaded environment?

`EpochDB` solves these problems out-of-the-box with a clean, simple API.

## ✨ Core Features

* **Intelligent Data Lifecycle:** Automatically prunes old, unused data based on a configurable grace period and frequency threshold.
* **Performance-First Architecture:** Uses a multi-tree system to separate "hot," frequently-updated metadata from "cold," larger data blobs. This maximizes `sled`'s page cache efficiency.
* **Concurrency-Safe by Design:** All core operations are thread-safe. Frequency counters are updated atomically using race-proof `compare-and-swap` loops.
* **Durable & Crash-Safe:** Inherits the industrial-strength durability and crash-safety guarantees of `sled`'s Write-Ahead Log.
* **Ergonomic API:** Provides a simple, high-level API that abstracts away the complexity of the underlying storage engine.

## 🚀 Quick Start

Get started with `EpochDB` by adding it to your `Cargo.toml`:

```toml
[dependencies]
epoch-db = "0.2.8"
````

### Basic Usage

```rust
use epoch_db::DB;
use std::path::Path;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open the database. It will be created if it doesn't exist.
    let db = DB::new(Path::new("./my_database"))?;

    // 2. Set a value with a 60-second Time-To-Live (TTL).
    // Metadata (frequency, created_at) is handled automatically.
    db.set("user:1", "Alice", Some(Duration::from_secs(60)))?;
    
    // 3. Get the value back.
    if let Some(value) = db.get("user:1")? {
        println!("Found value: {}", value); // "Found value: Alice"
    }

    // 4. Increment the frequency counter safely across multiple threads.
    db.increment_frequency("user:1")?;

    // 5. Remove data atomically from all trees.
    db.remove("user:1")?;

    Ok(())
}
```

## 🗺️ Roadmap

`EpochDB` is actively being developed. Our goal is to create the best tool for managing ephemeral and usage-tracked data in the Rust ecosystem.

  * **V1 (The Core Engine)**

      * [x] Two-tree architecture (`data_tree`, `metadata_tree`).
      * [x] Core API (`set`, `get`, `remove`, `increment_frequency`).
      * [x] Robust TTL / Data Lifecycle background thread.
      * [x] Polished documentation and examples.

  * **V2 (Production Readiness)**

      * [x] Simple, robust backup engine (`db.backup_to(...)`).
      * [x] Observability (expose performance metrics for Prometheus).
      * [x] Ergonomic, high-level transaction API.

  * **V3 (The Ecosystem)**

      * [ ] Optional networked server (gRPC or custom TCP protocol).
      * [ ] A simple CLI tool for database inspection and management.
      * [ ] A TUI or web-based dashboard for viewing stats.

## ❤️ Contributing

Contributions are welcome and greatly appreciated\! This project is a fantastic opportunity to dive into systems programming, database internals, and high-performance Rust.

If you're interested in helping, please:

1.  Fork the repository.
2.  Take a look at our [Roadmap](#roadmap) and the open [Issues](https://github.com/FabioCanavarro/EpochDB/issues). Issues marked with `[Help Wanted]` or `good first issue` are great places to start.
3.  Feel free to open a new issue to discuss a feature or a bug.
4.  Submit a pull request with your changes. Please ensure your code is tested and documented.

We are building a welcoming and collaborative community. Let's build something great together!

## 📜 License
This project is licensed under the [MIT license](http://opensource.org/licenses/MIT)
