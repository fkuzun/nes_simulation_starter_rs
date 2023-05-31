use std::collections::HashMap;
use std::error::Error;
use std::fmt::format;
use std::hash::Hasher;
use std::io::Write;
use std::os::unix::raw::{time_t, uid_t};
use std::process::{Child, Command, Stdio};
use std::time;
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use serde_json::Result;

#[derive(Deserialize, Debug)]
struct FixedTopology {
    //todo: check if we can just make that a tuple
    nodes: HashMap<usize, Vec<f64>>,
    children: HashMap<usize, Vec<usize>>
}

#[derive(Deserialize, Debug)]
struct ActualTopology {
    edges: Vec<Edge>,
    nodes: Vec<ActualNode>
}

#[derive(Deserialize, Debug)]
struct ActualNode {
available_resources: usize,
    id: usize,
    ip_address: String,
    location: Option<Vec<f64>>,
    nodeType: String
}

#[derive(Deserialize, Debug)]
struct Edge {
    source: usize,
    target: usize
}

#[derive(Deserialize, Debug)]
struct ConnectivityReply {
    statusCode: usize,
    success: bool
}

fn main() {
    let nes_build_directory = "/home/squirrel/nebulastream/cmake-build-release-s2/";
    let coordinator_path = nes_build_directory.to_owned() + "nes-core/nesCoordinator";
    let worker_path = nes_build_directory.to_owned() + "nes-core/nesWorker";
    let mut coordinator_process = Command::new(coordinator_path)
        .arg("--restServerCorsAllowedOrigin=http://localhost:3000")
        //.stdin(Stdio::inherit()) //todo for benchmarking: what are the performance implications of keeping these open for many processes?
        //.stdin(Stdio::piped()) //todo for benchmarking: what are the performance implications of keeping these open for many processes?
        .spawn()
        .expect("failed to execute coordinator");

    //wait until coordinator is online
    wait_for_coordinator().unwrap_or_else(|x| coordinator_process.kill().unwrap());
    std::thread::sleep(time::Duration::from_secs(1));

    let json_string = std::fs::read_to_string("3_layer_topology.json").unwrap();
    let topology: FixedTopology = serde_json::from_str(json_string.as_str()).unwrap();
    dbg!(topology);

    wait_for_topology(1).unwrap();

    //wait for user to press key to exit
    let input: String = text_io::read!("{}\n");

    //kill coordinator
    let exit_status = coordinator_process.wait().unwrap();
    println!("{}", exit_status.success());
}

fn start_fixed_location_workers(topology: &FixedTopology, parent_id: usize, worker_prcesses: &mut Vec<Child>, worker_path: &str) -> std::result::Result<(), Box<dyn Error>> {
     let id = parent_id + 1;
     let location = topology.nodes.get(&id).ok_or(format!("could not find node with id {}", id))?;
     worker_prcesses.push(Command::new(worker_path)
             .arg(format!("--fieldNodeLocationCoordinates={},{}", location[0], location[1]))
             .arg("--nodeSpatialType=FIXED_LOCATION")
             .spawn()
             .expect("failed to execute coordinator"));
    //todo: wait until worker has started

    for child in topology.children.to_owned() {
        start_fixed_location_workers(topology, id, worker_prcesses, worker_path)?;
    }
    Ok(())
}

fn wait_for_coordinator() -> std::result::Result<(), Box<dyn Error>> {
    loop {
        if let Ok(reply) = reqwest::blocking::get("http://127.0.0.1:8081/v1/nes/connectivity/check") {
            if reply.json::<ConnectivityReply>().unwrap().success {
                println!("connected");
                let input: String = text_io::read!("{}\n");
                break Ok(())
            }
            let input: String = text_io::read!("{}\n");
            std::thread::sleep(time::Duration::from_secs(1));
        }
        //todo: sleep
    }
}

fn wait_for_topology(expected_node_count: usize) -> std::result::Result<(), Box<dyn Error>> {
    loop {
        if let Ok(reply) = reqwest::blocking::get("http://127.0.0.1:8081/v1/nes/topology") {
            if reply.json::<ActualTopology>().unwrap().nodes.len() == expected_node_count {
                println!("topology contains {} nodes", expected_node_count);
                break Ok(())
            }
            println!("number of nodes not reached");
            std::thread::sleep(time::Duration::from_secs(1));
        }
        //todo: sleep
    }
}
