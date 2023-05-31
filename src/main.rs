use std::error::Error;
use std::os::unix::raw::time_t;
use std::process::Command;
use std::time;
use futures::executor::block_on;
fn main() {
    let nes_build_directory = "/home/squirrel/nebulastream/cmake-build-release-s2/";
    let coordinator_path = nes_build_directory.to_owned() + "nes-core/nesCoordinator";
    let mut coordinator_process = Command::new(coordinator_path)
        .arg("--restServerCorsAllowedOrigin=http://localhost:3000")
        .spawn()
        .expect("failed to execute coordinator");

    //wait for user to press key to exit
    let input: String = text_io::read!("{}\n");

    //kill coordinator
    coordinator_process.kill().unwrap();
    let coordinator_exit_code = coordinator_process.wait().expect("failed to wait for coordinator");
}

async fn wait_for_coordinator() -> Result<(), Box<dyn Error>> {
    loop {
        if let Ok(reply) = reqwest::get("http://127.0.0.1:8081/v1/nes//connectivity/check").await {
            if reply.json()["success"] {
                println!("connected");
                break Ok(())
            }
            println!("coordinator not online");
            std::thread::sleep(time::Duration::from_secs(1));
        }
        //todo: sleep
    }
}
