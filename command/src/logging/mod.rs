//! Sōzu logs, optimized for performance
//!
//! Instead of relying on well-known logging or tracing solutions,
//! Sōzu has its own logging stack that prioritizes CPU performance
//!
//! The `logs-cache` flag, on of that, saves lookup time by storing
//! the ENABLED status of each log call-site, in a `static mut`.
//! The gain in performance is measurable with a lot of log directives,
//! but mostly negligible, since CPUs are clever enough to recognize such patterns.

pub mod access_logs;
pub mod display;
#[macro_use]
pub mod logs;

use std::net::AddrParseError;

pub use crate::logging::access_logs::*;
pub use crate::logging::logs::*;

#[derive(thiserror::Error, Debug)]
pub enum LogError {
    #[error("invalid log target {0}: {1}")]
    InvalidLogTarget(String, &'static str),
    #[error("invalid log target {0}: {1}")]
    InvalidSocketAddress(String, AddrParseError),
    #[error("could not open log file {0}: {1}")]
    OpenFile(String, std::io::Error),
    #[error("could not connect to TCP socket {0}: {1}")]
    TcpConnect(String, std::io::Error),
    #[error("could not create unbound UNIX datagram: {0}")]
    CreateUnixSocket(std::io::Error),
    #[error("could not connect to UNIX datagram {0}: {1}")]
    ConnectToUnixSocket(String, std::io::Error),
    #[error("could not bind to UDP socket: {0}")]
    UdpBind(std::io::Error),
}
