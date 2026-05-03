use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{error, info, warn};

const KILL_CHAIN: &str = r#"ps -ef | grep 'nesCoordinator' | grep -v grep | awk '{print $2}' | xargs -r kill -9 && ps -ef | grep 'nesWorker' | grep -v grep | awk '{print $2}' | xargs -r kill -9 && ps -ef | grep 'tcp_input_server' | grep -v grep | awk '{print $2}' | xargs -r kill -9"#;

#[derive(Clone)]
struct AppState {
    child: Arc<Mutex<Option<Child>>>,
}

#[derive(Serialize)]
struct StopResponse {
    ok: bool,
}

#[derive(Serialize)]
struct StatusResponse {
    running: bool,
    pid: Option<u32>,
}

fn env_required(name: &str) -> Result<String, (StatusCode, String)> {
    std::env::var(name).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("missing required env var: {}", name),
        )
    })
}

async fn start_handler(State(state): State<AppState>) -> impl IntoResponse {
    info!("POST /start received");
    let mut guard = state.child.lock().unwrap();

    // Check if already running
    if let Some(ref mut child) = *guard {
        match child.try_wait() {
            Ok(None) => {
                warn!("POST /start rejected: simulator already running (pid={})", child.id());
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({"ok": false, "error": "simulator already running", "pid": child.id()})),
                )
                    .into_response();
            }
            _ => {
                info!("clearing stale child handle (process already exited)");
                *guard = None;
            }
        }
    }

    let sim_bin = match env_required("SIM_BIN") {
        Ok(v) => v,
        Err((status, msg)) => { error!("SIM_BIN not set"); return (status, msg).into_response(); },
    };
    let sim_type = match env_required("SIM_TYPE") {
        Ok(v) => v,
        Err((status, msg)) => { error!("SIM_TYPE not set"); return (status, msg).into_response(); },
    };
    let sim_nes_dir = match env_required("SIM_NES_DIR") {
        Ok(v) => v,
        Err((status, msg)) => { error!("SIM_NES_DIR not set"); return (status, msg).into_response(); },
    };
    let sim_toml = match env_required("SIM_TOML") {
        Ok(v) => v,
        Err((status, msg)) => { error!("SIM_TOML not set"); return (status, msg).into_response(); },
    };
    let sim_output_dir = match env_required("SIM_OUTPUT_DIR") {
        Ok(v) => v,
        Err((status, msg)) => { error!("SIM_OUTPUT_DIR not set"); return (status, msg).into_response(); },
    };
    let sim_tcp_input_bin = match env_required("SIM_TCP_INPUT_BIN") {
        Ok(v) => v,
        Err((status, msg)) => { error!("SIM_TCP_INPUT_BIN not set"); return (status, msg).into_response(); },
    };
    let sim_runs = match env_required("SIM_RUNS") {
        Ok(v) => v,
        Err((status, msg)) => { error!("SIM_RUNS not set"); return (status, msg).into_response(); },
    };

    let live_port = std::env::var("LIVE_LATENCY_PORT").unwrap_or_else(|_| "9001".to_string());

    info!("spawning simulator: bin={} type={} nes_dir={} toml={} output={} tcp_input={} runs={} live_port={}",
        sim_bin, sim_type, sim_nes_dir, sim_toml, sim_output_dir, sim_tcp_input_bin, sim_runs, live_port);

    let child_result = Command::new(&sim_bin)
        .args([
            &sim_type,
            &sim_nes_dir,
            &sim_toml,
            &sim_output_dir,
            &sim_tcp_input_bin,
            &sim_runs,
        ])
        .env("LIVE_LATENCY_PORT", &live_port)
        .stdout(Stdio::from(
            std::fs::File::create("/tmp/berlin-trams-sim.log").unwrap(),
        ))
        .stderr(Stdio::from(
            std::fs::File::create("/tmp/berlin-trams-sim.err").unwrap(),
        ))
        .spawn();

    match child_result {
        Ok(child) => {
            let pid = child.id();
            info!("simulator spawned successfully, pid={}", pid);

            let _ = std::fs::write("/tmp/berlin-trams-sim.pid", pid.to_string());

            *guard = Some(child);
            (StatusCode::OK, Json(serde_json::json!({"ok": true, "pid": pid}))).into_response()
        }
        Err(e) => {
            error!("failed to spawn simulator: {} (bin={})", e, sim_bin);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": format!("spawn failed: {}", e)})),
            )
                .into_response()
        }
    }
}

async fn stop_handler(State(state): State<AppState>) -> impl IntoResponse {
    info!("POST /stop received");
    let mut guard = state.child.lock().unwrap();

    if let Some(ref mut child) = *guard {
        let pid = child.id();
        info!("sending SIGINT to simulator pid={}", pid);

        unsafe {
            libc::kill(pid as i32, libc::SIGINT);
        }

        // Wait up to 5 seconds for exit
        let mut exited = false;
        for i in 0..50 {
            match child.try_wait() {
                Ok(Some(status)) => {
                    info!("simulator exited after SIGINT (status={}, waited ~{}ms)", status, i * 100);
                    exited = true;
                    break;
                }
                Ok(None) => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    warn!("try_wait error: {}", e);
                    exited = true;
                    break;
                }
            }
        }

        if !exited {
            warn!("simulator pid={} did not exit after 5s SIGINT, sending SIGKILL", pid);
            let _ = child.kill();
            let _ = child.wait();
            info!("simulator killed with SIGKILL");
        }
    } else {
        info!("POST /stop: no running simulator to stop");
    }

    *guard = None;
    let _ = std::fs::remove_file("/tmp/berlin-trams-sim.pid");

    info!("running kill chain for orphaned NES processes");
    match Command::new("sh").arg("-c").arg(KILL_CHAIN).status() {
        Ok(s) => info!("kill chain finished (status={})", s),
        Err(e) => warn!("kill chain failed: {}", e),
    }

    info!("POST /stop complete");
    Json(StopResponse { ok: true }).into_response()
}

async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    info!("GET /status received");
    let mut guard = state.child.lock().unwrap();

    let (running, pid) = if let Some(ref mut child) = *guard {
        match child.try_wait() {
            Ok(None) => (true, Some(child.id())),
            Ok(Some(status)) => {
                info!("GET /status: child already exited (status={}), clearing handle", status);
                *guard = None;
                (false, None)
            }
            Err(e) => {
                warn!("GET /status: try_wait error: {}", e);
                *guard = None;
                (false, None)
            }
        }
    } else {
        (false, None)
    };

    info!("GET /status -> running={} pid={:?}", running, pid);
    Json(StatusResponse { running, pid })
}

async fn healthz_handler() -> impl IntoResponse {
    info!("GET /healthz");
    StatusCode::OK
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let port: u16 = std::env::var("DAEMON_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9000);

    info!("berlin-trams-daemon starting");
    info!("  DAEMON_PORT = {}", port);
    info!("  SIM_BIN = {:?}", std::env::var("SIM_BIN").ok());
    info!("  SIM_TYPE = {:?}", std::env::var("SIM_TYPE").ok());
    info!("  SIM_NES_DIR = {:?}", std::env::var("SIM_NES_DIR").ok());
    info!("  SIM_TOML = {:?}", std::env::var("SIM_TOML").ok());
    info!("  SIM_OUTPUT_DIR = {:?}", std::env::var("SIM_OUTPUT_DIR").ok());
    info!("  SIM_TCP_INPUT_BIN = {:?}", std::env::var("SIM_TCP_INPUT_BIN").ok());
    info!("  SIM_RUNS = {:?}", std::env::var("SIM_RUNS").ok());
    info!("  LIVE_LATENCY_PORT = {:?}", std::env::var("LIVE_LATENCY_PORT").ok());

    let state = AppState {
        child: Arc::new(Mutex::new(None)),
    };

    let app = Router::new()
        .route("/start", post(start_handler))
        .route("/stop", post(stop_handler))
        .route("/status", get(status_handler))
        .route("/healthz", get(healthz_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .expect("failed to bind daemon port");

    info!("daemon ready, listening on 0.0.0.0:{}", port);
    axum::serve(listener, app).await.unwrap();
}
