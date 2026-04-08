use std::error::Error;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::{fs, time};

use chrono::Local;
use nes_tools::topology::{AddEdgeReply, AddEdgeRequest};

use crate::nes_types::{ActualTopology, ConnectivityReply};

/// Restructures the NES topology by re-parenting nodes according to the provided edge list.
/// For each (parent, child) pair where parent != 1, adds the child under the new parent
/// and removes the default edge from node 1.
pub fn add_edges_from_list(rest_port: &u16, edges: &Vec<(u64, u64)>) -> Result<(), Box<dyn Error>> {
    let client = reqwest::blocking::Client::new();
    for (parent_id, child_id) in edges {
        if parent_id == &1 {
            continue;
        }
        println!("adding edge from {} to {}", parent_id, child_id);
        let link_request = AddEdgeRequest {
            parent_id: *parent_id,
            child_id: *child_id,
        };
        let result = client
            .post(format!(
                "http://127.0.0.1:{}/v1/nes/topology/addAsChild",
                &rest_port.to_string()
            ))
            .json(&link_request)
            .send()?;
        let reply: AddEdgeReply = result.json()?;
        if !reply.success {
            return Err("Could not add edge".into());
        }
        let link_request = AddEdgeRequest {
            parent_id: 1,
            child_id: *child_id,
        };
        let result = client
            .delete("http://127.0.0.1:8081/v1/nes/topology/removeAsChild")
            .json(&link_request)
            .send()?;
        println!("sleeping");
        sleep(Duration::from_millis(200));
        let reply: AddEdgeReply = result.json()?;
        if !reply.success {
            return Err("Could not add edge".into());
        }
    }
    Ok(())
}

pub(crate) fn create_folder_with_timestamp(mut path: PathBuf, prefix: &str) -> PathBuf {
    let current_time: chrono::DateTime<Local> = Local::now();
    let formatted_timestamp = current_time.format("%Y-%m-%d_%H-%M-%S").to_string();
    let folder_name = format!("{}{}", prefix, formatted_timestamp);
    path.push(folder_name);
    fs::create_dir_all(&path).expect("Failed to create folder");
    println!("Folder created: {}", path.display());
    path
}

/// Polls the coordinator connectivity endpoint until the coordinator reports success.
pub(crate) fn wait_for_coordinator(
    shutdown_triggered: Arc<AtomicBool>,
) -> std::result::Result<(), Box<dyn Error>> {
    for _i in 0..10 {
        if shutdown_triggered.load(Ordering::SeqCst) {
            return Err(String::from("Shutdown triggered").into());
        }
        if let Ok(reply) =
            reqwest::blocking::get("http://127.0.0.1:8081/v1/nes/connectivity/check")
        {
            if reply.json::<ConnectivityReply>().unwrap().success {
                println!("Coordinator has connected");
                return Ok(());
            }
        }
        sleep(Duration::from_secs(1));
    }
    println!("Coordinator did not connect");
    Err(String::from("Coordinator did not connect").into())
}

/// Waits until the NES topology contains the expected number of nodes.
pub(crate) fn wait_for_topology(
    expected_node_count: Option<usize>,
    shutdown_triggered: Arc<AtomicBool>,
    restPort: u16,
) -> std::result::Result<usize, Box<dyn Error>> {
    println!(
        "waiting for topology, rest port {}",
        &restPort.to_string()
    );
    for _i in 0..10 {
        if shutdown_triggered.load(Ordering::SeqCst) {
            return Err(String::from("Shutdown triggered").into());
        }
        if let Ok(reply) =
            reqwest::blocking::get(format!("http://127.0.0.1:{}/v1/nes/topology", restPort))
        {
            let size = reply.json::<ActualTopology>().unwrap().nodes.len();
            println!("topology contains {} nodes", size);
            if let Some(expected) = expected_node_count {
                if size == expected {
                    return Ok(size);
                }
                println!("number of nodes not reached, expected {}", expected);
            }
        }
        std::thread::sleep(time::Duration::from_secs(1));
    }
    Err(String::from("Expected node count not reached in topology").into())
}

pub fn print_topology(restPort: u16) -> std::result::Result<(), Box<dyn Error>> {
    println!(
        "retrieving topology from, rest port {}",
        &restPort.to_string()
    );
    if let Ok(reply) =
        reqwest::blocking::get(format!("http://127.0.0.1:{}/v1/nes/topology", restPort))
    {
        println!("{}", reply.text()?);
    }
    Ok(())
}

/// Calculates expected join output count based on the number of input tuples,
/// window size and join match interval. Only tuples whose sequence number
/// is divisible by join_match_interval produce a match.
pub fn get_expected_join_output_count(
    num_tuples: u64,
    window_size: u64,
    join_match_interval: u64,
) -> u64 {
    println!("num tuples: {}", num_tuples);
    let finished_windows = ((num_tuples - 1) / window_size) - 1;
    println!("finished windows: {}", finished_windows);
    let processed_tuples = finished_windows * window_size;
    println!("processed tuples: {}", processed_tuples);

    let matched_tuples = ((processed_tuples - 1) / join_match_interval) + 1;
    println!("matched tuples: {}", matched_tuples);

    matched_tuples
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_count_calculation() {
        let num_tuples = 600 / 2;
        let window_size = 40;
        let join_match_interval = 2;
        let expected_output_count =
            get_expected_join_output_count(num_tuples, window_size, join_match_interval);
        assert_eq!(expected_output_count, 140);

        let num_tuples = 600 / 2;
        let window_size = 10;
        let join_match_interval = 2;
        let expected_output_count =
            get_expected_join_output_count(num_tuples, window_size, join_match_interval);
        assert_eq!(expected_output_count, 145);

        let num_tuples = 600 / 2;
        let window_size = 40;
        let join_match_interval = 7;
        let expected_output_count =
            get_expected_join_output_count(num_tuples, window_size, join_match_interval);
        assert_eq!(expected_output_count, 40);

        let num_tuples = 600 / 2;
        let window_size = 5;
        let join_match_interval = 7;
        let expected_output_count =
            get_expected_join_output_count(num_tuples, window_size, join_match_interval);
        assert_eq!(expected_output_count, 43);

        let num_tuples = 600 / 2;
        let window_size = 1;
        let join_match_interval = 1;
        let expected_output_count =
            get_expected_join_output_count(num_tuples, window_size, join_match_interval);
        assert_eq!(expected_output_count, 299);
    }
}
