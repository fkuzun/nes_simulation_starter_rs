use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

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

#[derive(Deserialize, Debug, Default)]
#[serde(default)]
struct StartReq {
    #[serde(rename = "totalNodes")]
    total_nodes: Option<u32>,
    #[serde(rename = "mobileNodes")]
    mobile_nodes: Option<u32>,
    #[serde(rename = "topoChangeMs")]
    topo_change_ms: Option<u32>,
    #[serde(rename = "reconfigMode")]
    reconfig_mode: Option<String>,
    #[serde(rename = "queryMode")]
    query_mode: Option<String>,
}

fn render_toml(template: &str, folder: &Path, req: &StartReq) -> Result<String, String> {
    let total = req.total_nodes.ok_or("missing totalNodes")?;
    let mobile = req.mobile_nodes.ok_or("missing mobileNodes")?;
    let topo_ms = req.topo_change_ms.ok_or("missing topoChangeMs")?;
    let reconfig = req.reconfig_mode.as_deref().ok_or("missing reconfigMode")?;

    // topoChangeMs -> speedup_factor (500->0.5, 1000->1, 2000->2, 4000->4)
    let speedup = (topo_ms as f64) / 1000.0;

    // reconfigMode -> two booleans
    let (enable_reconfig, enable_proactive) = match reconfig {
        "holistic" => (false, false),
        "incremental" => (true, true),
        other => return Err(format!("unknown reconfigMode: {}", other)),
    };

    // The simulator handles these three fields with two different mechanisms:
    //   - `fixed_topology_nodes` and `TrajectoriesDir` deserialize as
    //     RelativePathBuf and are joined against `base_path` (set to the parent
    //     of the input config file). They MUST be relative.
    //   - `place_default_sources_on_node_ids_path` deserializes as a plain
    //     PathBuf and is read directly (simulator/src/query.rs:13), with no
    //     base_path resolution. It MUST be absolute (or relative to the
    //     simulator's cwd, which we don't control).
    // We write the rendered TOML inside `folder`, so the relative names below
    // refer to files sitting next to it.
    let folder_abs = folder
        .canonicalize()
        .map_err(|e| format!("canonicalize {}: {}", folder.display(), e))?;
    let folder_str = folder_abs
        .to_str()
        .ok_or("folder path is not valid UTF-8")?;
    let source_groups = format!("{}/source_groups.json", folder_str);
    let fixed_topology = "fixed_topology.json";
    let trajectories_dir = ".";

    let mut out = template.to_string();

    let mut sub = |pattern: &str, replacement: &str| -> Result<(), String> {
        let re = regex::Regex::new(pattern).map_err(|e| format!("regex: {}", e))?;
        if !re.is_match(&out) {
            return Err(format!("pattern not found in TOML: {}", pattern));
        }
        out = re.replace(&out, replacement).into_owned();
        Ok(())
    };

    sub(
        r"(?m)^enable_query_reconfiguration\s*=\s*\[[^\]]*\]",
        &format!("enable_query_reconfiguration = [{}]", enable_reconfig),
    )?;
    sub(
        r"(?m)^enable_proactive_deployment\s*=\s*\[[^\]]*\]",
        &format!("enable_proactive_deployment = [{}]", enable_proactive),
    )?;
    sub(
        r"(?m)^speedup_factor\s*=\s*\[[^\]]*\]",
        &format!("speedup_factor = [{}]", speedup),
    )?;
    sub(
        r"(?m)^placementAmendmentThreadCount\s*=\s*\[[^\]]*\]",
        "placementAmendmentThreadCount = [8]",
    )?;
    sub(
        r#"(?m)^(\s*)place_default_sources_on_node_ids_path\s*=\s*"[^"]*""#,
        &format!(
            r#"${{1}}place_default_sources_on_node_ids_path = "{}""#,
            source_groups
        ),
    )?;
    sub(
        r#"(?m)^(\s*)fixed_topology_nodes\s*=\s*"[^"]*""#,
        &format!(r#"${{1}}fixed_topology_nodes = "{}""#, fixed_topology),
    )?;
    sub(
        r#"(?m)^(\s*)TrajectoriesDir\s*=\s*"[^"]*""#,
        &format!(r#"${{1}}TrajectoriesDir = "{}""#, trajectories_dir),
    )?;

    info!(
        "rendered TOML: totalNodes={} mobileNodes={} speedup_factor={} enable_query_reconfiguration={} enable_proactive_deployment={} folder={}",
        total, mobile, speedup, enable_reconfig, enable_proactive, folder.display()
    );

    Ok(out)
}

async fn start_handler(
    State(state): State<AppState>,
    Json(req): Json<StartReq>,
) -> impl IntoResponse {
    info!("POST /start received: {:?}", req);
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

    let sim_type = match req.query_mode.as_deref() {
        Some("stateful") => "STATEFUL".to_string(),
        Some("stateless") => "STATELESS".to_string(),
        Some(other) => {
            error!("unknown queryMode: {}", other);
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"ok": false, "error": format!("unknown queryMode: {}", other)})),
            )
                .into_response();
        }
        None => std::env::var("SIM_TYPE").unwrap_or_else(|_| "STATEFUL".to_string()),
    };

    // Config tree root = directory holding the generic SIM_TOML.
    // (Name-agnostic: works whether the tree is experiment_input/ or experiment_input_new/.)
    let template_path = PathBuf::from(&sim_toml);
    let exp_root = match template_path.parent().map(|p| p.to_path_buf()) {
        Some(p) => p,
        None => {
            error!("cannot derive config tree root from SIM_TOML={}", sim_toml);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": "cannot derive config tree root from SIM_TOML"})),
            )
                .into_response();
        }
    };

    // Resolve configuration folder under variable_reconnect_speeds/
    let total = match req.total_nodes {
        Some(v) => v,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"ok": false, "error": "missing totalNodes"})),
            )
                .into_response();
        }
    };
    let mobile = match req.mobile_nodes {
        Some(v) => v,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"ok": false, "error": "missing mobileNodes"})),
            )
                .into_response();
        }
    };
    // Layout: <exp_root>/<totalNodes>/<mobileNodes>_<totalNodes>/
    let inner = format!("{}_{}", mobile, total);
    let folder = exp_root.join(total.to_string()).join(&inner);
    if !folder.is_dir() {
        warn!("configuration folder not found: {}", folder.display());
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "ok": false,
                "error": format!("configuration not found: {}", folder.display())
            })),
        )
            .into_response();
    }

    // Render the template TOML against the request
    let template = match std::fs::read_to_string(&sim_toml) {
        Ok(s) => s,
        Err(e) => {
            error!("failed to read SIM_TOML {}: {}", sim_toml, e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": format!("read SIM_TOML: {}", e)})),
            )
                .into_response();
        }
    };
    let rendered = match render_toml(&template, &folder, &req) {
        Ok(s) => s,
        Err(e) => {
            error!("render_toml failed: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"ok": false, "error": format!("render TOML: {}", e)})),
            )
                .into_response();
        }
    };

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    // Write rendered TOML inside `folder` so the simulator's base_path
    // (parent of input config) resolves data files like fixed_topology.json
    // and source_groups.json alongside it.
    let rendered_path = format!("{}/sim_rendered_{}.toml", folder.display(), ts);
    if let Err(e) = std::fs::write(&rendered_path, &rendered) {
        error!("failed to write rendered TOML {}: {}", rendered_path, e);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"ok": false, "error": format!("write rendered TOML: {}", e)})),
        )
            .into_response();
    }
    info!(
        "rendered TOML written to {}:\n----- BEGIN RENDERED TOML -----\n{}\n----- END RENDERED TOML -----",
        rendered_path, rendered
    );

    info!("spawning simulator: bin={} type={} nes_dir={} toml={} output={} tcp_input={} runs={} live_port={}",
        sim_bin, sim_type, sim_nes_dir, rendered_path, sim_output_dir, sim_tcp_input_bin, sim_runs, live_port);

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
            &rendered_path,
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
    // Layered subscriber: stdout (always) + optional file at $SIM_OUTPUT_DIR/daemon.log.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stdout_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stdout);

    let file_guard = match std::env::var("SIM_OUTPUT_DIR") {
        Ok(dir) => match std::fs::create_dir_all(&dir) {
            Ok(()) => {
                let appender = tracing_appender::rolling::never(&dir, "daemon.log");
                let (non_blocking, guard) = tracing_appender::non_blocking(appender);
                let file_layer = tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_ansi(false);
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(stdout_layer)
                    .with(file_layer)
                    .init();
                info!("daemon log file: {}/daemon.log", dir);
                Some(guard)
            }
            Err(e) => {
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(stdout_layer)
                    .init();
                warn!("could not create SIM_OUTPUT_DIR={}: {} (stdout-only logging)", dir, e);
                None
            }
        },
        Err(_) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(stdout_layer)
                .init();
            warn!("SIM_OUTPUT_DIR not set at daemon startup (stdout-only logging)");
            None
        }
    };
    // Keep the appender guard alive for the lifetime of main so writes flush on exit.
    let _file_guard = file_guard;

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
