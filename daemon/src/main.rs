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

    if let Err(e) = std::fs::create_dir_all(&sim_output_dir) {
        error!("failed to create output dir {}: {}", sim_output_dir, e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"ok": false, "error": format!("mkdir {} failed: {}", sim_output_dir, e)})),
        )
            .into_response();
    }

    let run_log_path = format!("{}/run.log", sim_output_dir);
    let run_pid_path = format!("{}/run.pid", sim_output_dir);

    let run_log = match std::fs::File::create(&run_log_path) {
        Ok(f) => f,
        Err(e) => {
            error!("failed to create {}: {}", run_log_path, e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": format!("create {} failed: {}", run_log_path, e)})),
            )
                .into_response();
        }
    };
    let run_log_err = match run_log.try_clone() {
        Ok(f) => f,
        Err(e) => {
            error!("failed to clone {} fd: {}", run_log_path, e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": format!("clone fd failed: {}", e)})),
            )
                .into_response();
        }
    };

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
        .stdout(Stdio::from(run_log))
        .stderr(Stdio::from(run_log_err))
        .spawn();

    match child_result {
        Ok(child) => {
            let pid = child.id();
            info!("simulator spawned successfully, pid={} (log={} pid_file={})", pid, run_log_path, run_pid_path);

            let _ = std::fs::write(&run_pid_path, pid.to_string());

            *guard = Some(child);
            (StatusCode::OK, Json(serde_json::json!({"ok": true, "pid": pid, "log": run_log_path, "pid_file": run_pid_path}))).into_response()
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
    if let Ok(dir) = std::env::var("SIM_OUTPUT_DIR") {
        let _ = std::fs::remove_file(format!("{}/run.pid", dir));
    }

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

async fn deployment_handler() -> impl IntoResponse {
    info!("GET /deployment received");

    let sim_output_dir = match std::env::var("SIM_OUTPUT_DIR") {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "SIM_OUTPUT_DIR not set"})),
            )
                .into_response();
        }
    };

    let log_path = format!("{}/run.log", sim_output_dir);
    let raw = match std::fs::read_to_string(&log_path) {
        Ok(s) => s,
        Err(e) => {
            warn!("GET /deployment: read {} failed: {}", log_path, e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("read {}: {}", log_path, e)})),
            )
                .into_response();
        }
    };

    let ansi = regex::Regex::new(r"\x1B\[[0-9;]*[a-zA-Z]").unwrap();
    let cleaned = ansi.replace_all(&raw, "");

    let latest: &str = match cleaned.rsplit_once("Starting experiment") {
        Some((_, tail)) => tail,
        None => cleaned.as_ref(),
    };

    let isqp_re = regex::Regex::new(
        r"Total Time to process ISQP Request=(\d+); placementTime=(\d+); deploymentTime=(\d+)",
    )
    .unwrap();

    let mut request_ns: u64 = 0;
    let mut placement_ns: u64 = 0;
    let mut deployment_ns: u64 = 0;
    let mut count: u64 = 0;
    for cap in isqp_re.captures_iter(latest) {
        request_ns    += cap[1].parse::<u64>().unwrap_or(0);
        placement_ns  += cap[2].parse::<u64>().unwrap_or(0);
        deployment_ns += cap[3].parse::<u64>().unwrap_or(0);
        count += 1;
    }

    let to_sec = |ns: u64| (ns as f64) / 1_000_000_000.0;
    let deploy = to_sec(deployment_ns);
    let opt_total = to_sec(request_ns);
    let placement = to_sec(placement_ns);
    let other_opt = (opt_total - deploy - placement).max(0.0);

    info!(
        "GET /deployment -> deploy={:.3}s optTotal={:.3}s placement={:.3}s otherOpt={:.3}s reconfigurations={}",
        deploy, opt_total, placement, other_opt, count
    );

    Json(serde_json::json!({
        "deploy": deploy,
        "optTotal": opt_total,
        "placement": placement,
        "otherOpt": other_opt,
        "reconfigurations": count,
    }))
    .into_response()
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
        .route("/deployment", get(deployment_handler))
        .route("/healthz", get(healthz_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .expect("failed to bind daemon port");

    info!("daemon ready, listening on 0.0.0.0:{}", port);
    axum::serve(listener, app).await.unwrap();
}
