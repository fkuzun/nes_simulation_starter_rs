use execute::Execute;
use itertools::Itertools;
pub mod stateless_simulation;
pub mod lib_stateless;
// use lib_stateless::*;

use std::error::Error;
use std::future::Future;
use std::io::Write;
use std::ops::Add;
use std::path::PathBuf;
use std::env;
use serde::{Deserialize, Serialize};
use crate::stateless_simulation::run_stateless_simulation;
pub mod stateful_simulation;
pub mod lib_stateful;
pub mod analyze;
pub mod rest_node_relocation;
pub mod MobileDeviceQuadrants;


#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum LogLevel {
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR,
    LOG_NONE,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 5 || args.len() > 8 {
        eprintln!("Usage: {} <nes directory> <experiment input config path> <output directory> <tcp input server executable> <number of runs> <log level (optional)>, <experiment path for retrial (optional)>", args[0]);
        std::process::exit(1);
    }

    let nes_root_dir = PathBuf::from(&args[1]);
    let input_config_path = PathBuf::from(&args[2]);
    let output_directory = PathBuf::from(&args[3]);
    let input_server_path = PathBuf::from(&args[4]);
    let runs: u64 = args[5].parse().unwrap();
    let log_level: LogLevel = if args.len() >= 7 {
        println!("Log level: {}", &args[6]);
        serde_json::from_str::<LogLevel>(&format!("\"{}\"", &args[6])).unwrap_or_else(|e| {
            eprintln!("Could not parse log level: {}", e);
            LogLevel::LOG_ERROR
        })
    } else {
        LogLevel::LOG_ERROR
    };
    let run_for_retrial_path = if args.len() == 8 {
        Some(PathBuf::from(&args[7]))
    } else {
        None
    };

    // run_stateful_simulation(nes_root_dir, input_config_path, &output_directory, &input_server_path, runs, &log_level, run_for_retrial_path)?;
    run_stateless_simulation(nes_root_dir, input_config_path, &output_directory, &input_server_path, runs, &log_level, run_for_retrial_path)?;
    Ok(())
}
