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
    let mut guard = state.child.lock().unwrap();

    // Check if already running
    if let Some(ref mut child) = *guard {
        match child.try_wait() {
            Ok(None) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({"ok": false, "error": "simulator already running", "pid": child.id()})),
                )
                    .into_response();
            }
            _ => {
                // Process exited, clear stale handle
                *guard = None;
            }
        }
    }

    let sim_bin = match env_required("SIM_BIN") {
        Ok(v) => v,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    let sim_type = match env_required("SIM_TYPE") {
        Ok(v) => v,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    let sim_nes_dir = match env_required("SIM_NES_DIR") {
        Ok(v) => v,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    let sim_toml = match env_required("SIM_TOML") {
        Ok(v) => v,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    let sim_output_dir = match env_required("SIM_OUTPUT_DIR") {
        Ok(v) => v,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    let sim_tcp_input_bin = match env_required("SIM_TCP_INPUT_BIN") {
        Ok(v) => v,
        Err((status, msg)) => return (status, msg).into_response(),
    };
    let sim_runs = match env_required("SIM_RUNS") {
        Ok(v) => v,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    let live_port = std::env::var("LIVE_LATENCY_PORT").unwrap_or_else(|_| "9001".to_string());

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
            info!("simulator started with pid {}", pid);

            // Write pidfile
            let _ = std::fs::write("/tmp/berlin-trams-sim.pid", pid.to_string());

            *guard = Some(child);
            (StatusCode::OK, Json(serde_json::json!({"ok": true, "pid": pid}))).into_response()
        }
        Err(e) => {
            error!("failed to spawn simulator: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": format!("spawn failed: {}", e)})),
            )
                .into_response()
        }
    }
}

async fn stop_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut guard = state.child.lock().unwrap();

    if let Some(ref mut child) = *guard {
        let pid = child.id();
        info!("stopping simulator pid {}", pid);

        // Send SIGINT for orderly shutdown
        unsafe {
            libc::kill(pid as i32, libc::SIGINT);
        }

        // Wait up to 5 seconds for exit
        let mut exited = false;
        for _ in 0..50 {
            match child.try_wait() {
                Ok(Some(_)) => {
                    exited = true;
                    break;
                }
                Ok(None) => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(_) => {
                    exited = true;
                    break;
                }
            }
        }

        if !exited {
            warn!("simulator did not exit after SIGINT, sending SIGKILL");
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    *guard = None;
    let _ = std::fs::remove_file("/tmp/berlin-trams-sim.pid");

    // Run kill chain to clean up any orphaned processes
    info!("running kill chain for orphaned processes");
    let _ = Command::new("sh").arg("-c").arg(KILL_CHAIN).status();

    Json(StopResponse { ok: true }).into_response()
}

async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut guard = state.child.lock().unwrap();

    let (running, pid) = if let Some(ref mut child) = *guard {
        match child.try_wait() {
            Ok(None) => (true, Some(child.id())),
            _ => {
                *guard = None;
                (false, None)
            }
        }
    } else {
        (false, None)
    };

    Json(StatusResponse { running, pid })
}

async fn healthz_handler() -> impl IntoResponse {
    StatusCode::OK
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let port: u16 = std::env::var("DAEMON_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9000);

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

    info!("daemon listening on 0.0.0.0:{}", port);
    axum::serve(listener, app).await.unwrap();
}
