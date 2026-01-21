use tracing_subscriber::{
    fmt,
    EnvFilter
};

use crate::db::errors::TransientError;

pub async fn check_argument(
    command: String,
    expected: u32,
    received: u32,
    min_expected: Option<u32>
) -> Result<(), TransientError> {
    if let Some(min) = min_expected {
        if received < min {
            return Err(TransientError::WrongNumberOfArguments {
                command,
                expected: min,
                received
            });
        }
    } else if received > expected {
        return Err(TransientError::WrongNumberOfArguments {
            command,
            expected,
            received
        });
    }
    Ok(())
}

pub fn init_logger(default_val: String) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_val));

    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .with_line_number(true)
        .with_target(true)
        .compact()
        .init();
}
