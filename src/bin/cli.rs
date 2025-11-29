use clap::{Parser, Subcommand};

// Cli Parser
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(short, long)]
    addr: String,
}

fn main() {
    todo!();
}
