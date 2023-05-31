use std::collections::HashMap;
use std::error::Error;
use std::hash::Hasher;
use std::io::Write;
use std::os::unix::raw::time_t;
use std::process::{Command, Stdio};
use std::time;
use futures::executor::block_on;
fn main() {
    let nes_build_directory = "/home/squirrel/nebulastream/cmake-build-release-s2/";
    let coordinator_path = nes_build_directory.to_owned() + "nes-core/nesCoordinator";
    let mut coordinator_process = Command::new(coordinator_path)
        .arg("--restServerCorsAllowedOrigin=http://localhost:3000")
        //.stdin(Stdio::inherit()) //todo for benchmarking: what are the performance implications of keeping these open for many processes?
        .stdin(Stdio::piped()) //todo for benchmarking: what are the performance implications of keeping these open for many processes?
        .spawn()
        .expect("failed to execute coordinator");

    //wait until coordinator is online
    //wait_for_coordinator().unwrap_or_else(|x| coordinator_process.kill().unwrap());
    std::thread::sleep(time::Duration::from_secs(1));

    //wait for user to press key to exit
    let input: String = text_io::read!("{}\n");

    //kill coordinator
    //coordinator_process.kill().unwrap();
    //coordinator_process.stdin.as_ref().unwrap().write_all(ctrlc::Signal::SIGTERM.as_str().as_bytes()).unwrap();
    //std::io::stdout().write_all(ctrlc::Signal::SIGTERM.as_str().as_bytes()).unwrap();
    let exit_status = coordinator_process.wait().unwrap();
    println!("{}", exit_status);
}

fn wait_for_coordinator() -> Result<(), Box<dyn Error>> {
    loop {
        if let Ok(reply) = block_on(reqwest::get("http://127.0.0.1:8081/v1/nes//connectivity/check")) {
            if block_on(reply.json::<HashMap<String, bool>>()).unwrap()["success"] {
                println!("connected");
                let input: String = text_io::read!("{}\n");
                println!("{}", input);
                println!("connected");
                break Ok(())
            }
            let input: String = text_io::read!("{}\n");
            println!("coordinator not online");
            std::thread::sleep(time::Duration::from_secs(1));
        }
        //todo: sleep
    }
}
