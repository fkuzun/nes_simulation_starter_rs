use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

const KILL_CHAIN: &str = r#"ps -ef | grep 'nesCoordinator' | grep -v grep | awk '{print $2}' | xargs -r kill -9 && ps -ef | grep 'nesWorker' | grep -v grep | awk '{print $2}' | xargs -r kill -9 && ps -ef | grep 'tcp_input_server' | grep -v grep | awk '{print $2}' | xargs -r kill -9"#;

#[derive(Clone)]
struct AppState {
    /// PID of the simulator process on the backend host (fat-2).
    remote_pid: Arc<Mutex<Option<u32>>>,
    /// Local `latency_service` child process. Spawned on /start, killed on
    /// /stop (and on the next /start, so each experiment gets a fresh sink).
    latency_child: Arc<Mutex<Option<Child>>>,
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

fn sh_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

fn ssh_run(host: &str, script: &str) -> Result<Output, String> {
    let mut child = Command::new("ssh")
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
            host,
            "bash",
            "-l",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("ssh spawn failed: {}", e))?;

    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| "ssh stdin not available".to_string())?;
        stdin
            .write_all(script.as_bytes())
            .map_err(|e| format!("ssh stdin write failed: {}", e))?;
    }
    drop(child.stdin.take());

    child
        .wait_with_output()
        .map_err(|e| format!("ssh wait failed: {}", e))
}

fn ssh_remote_running(host: &str, pid: u32) -> bool {
    match ssh_run(host, &format!("kill -0 {} 2>/dev/null", pid)) {
        Ok(out) => out.status.success(),
        Err(_) => false,
    }
}

/// Send SIGINT to a local pid using /bin/kill so we don't drag in libc just
/// for one syscall. Best-effort.
fn local_sigint(pid: u32) {
    let _ = Command::new("kill")
        .args(["-INT", &pid.to_string()])
        .status();
}

/// Stop the local `latency_service` child gracefully: SIGINT to let it flush
/// the final bucket and emit EOF, wait up to 5 s, then SIGKILL if still
/// alive. Always clears the slot.
fn stop_latency_service(child_slot: &Arc<Mutex<Option<Child>>>) {
    let mut guard = child_slot.lock().unwrap();
    let Some(mut child) = guard.take() else {
        return;
    };

    let pid = child.id();
    info!("stopping latency_service (pid={})", pid);
    local_sigint(pid);

    for i in 0..50 {
        std::thread::sleep(Duration::from_millis(100));
        match child.try_wait() {
            Ok(Some(status)) => {
                info!(
                    "latency_service exited after SIGINT (status={}, waited ~{}ms)",
                    status,
                    (i + 1) * 100
                );
                return;
            }
            Ok(None) => continue,
            Err(e) => {
                warn!("latency_service try_wait error: {}", e);
                break;
            }
        }
    }

    warn!(
        "latency_service pid={} did not exit after 5s SIGINT, sending SIGKILL",
        pid
    );
    let _ = child.kill();
    let _ = child.wait();
}

/// Spawn a fresh `latency_service` process on the daemon host (c10). The
/// SIM_TYPE env var controls tuple parsing; LATENCY_SINK_PORT is where the
/// simulator's forwarder on fat-2 will connect; LIVE_LATENCY_PORT is where
/// the bridge subscribes.
fn start_latency_service(
    sim_type: &str,
    sink_port: &str,
    live_port: &str,
) -> Result<Child, String> {
    let bin = std::env::var("LATENCY_SERVICE_BIN")
        .map_err(|_| "LATENCY_SERVICE_BIN not set".to_string())?;

    let log_dir = std::env::var("DAEMON_LOG_DIR").ok();
    let (stdout, stderr) = match log_dir {
        Some(ref dir) => {
            let _ = std::fs::create_dir_all(dir);
            let path = format!("{}/latency_service.log", dir);
            match std::fs::File::create(&path) {
                Ok(f) => {
                    let f2 = f.try_clone().map_err(|e| format!("clone fd: {}", e))?;
                    info!("latency_service logging to {}", path);
                    (Stdio::from(f), Stdio::from(f2))
                }
                Err(e) => {
                    warn!(
                        "could not open {}: {} (inheriting stdio)",
                        path, e
                    );
                    (Stdio::inherit(), Stdio::inherit())
                }
            }
        }
        None => (Stdio::inherit(), Stdio::inherit()),
    };

    info!(
        "spawning latency_service: bin={} SIM_TYPE={} LATENCY_SINK_PORT={} LIVE_LATENCY_PORT={}",
        bin, sim_type, sink_port, live_port
    );

    Command::new(&bin)
        .env("SIM_TYPE", sim_type)
        .env("LATENCY_SINK_PORT", sink_port)
        .env("LIVE_LATENCY_PORT", live_port)
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .map_err(|e| format!("spawn {} failed: {}", bin, e))
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

fn render_toml(template: &str, folder: &PathBuf, req: &StartReq) -> Result<String, String> {
    let total = req.total_nodes.ok_or("missing totalNodes")?;
    let mobile = req.mobile_nodes.ok_or("missing mobileNodes")?;
    let topo_ms = req.topo_change_ms.ok_or("missing topoChangeMs")?;
    let reconfig = req.reconfig_mode.as_deref().ok_or("missing reconfigMode")?;

    let speedup = (topo_ms as f64) / 1000.0;

    let (enable_reconfig, enable_proactive) = match reconfig {
        "holistic" => (false, false),
        "incremental" => (true, false),
        other => return Err(format!("unknown reconfigMode: {}", other)),
    };

    // `folder` is the absolute remote path on fat2 (built from SIM_TOML's
    // parent + totalNodes/mobile_total). We can't canonicalize here because
    // the filesystem is on a different host; the absolute join is sufficient.
    let folder_str = folder
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

    let host = match env_required("REMOTE_HOST") {
        Ok(v) => v,
        Err((s, m)) => {
            error!("REMOTE_HOST not set");
            return (s, m).into_response();
        }
    };

    let mut guard = state.remote_pid.lock().unwrap();

    if let Some(pid) = *guard {
        if ssh_remote_running(&host, pid) {
            warn!("POST /start rejected: simulator already running (pid={})", pid);
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "ok": false,
                    "error": "simulator already running",
                    "pid": pid
                })),
            )
                .into_response();
        } else {
            info!("clearing stale remote pid (pid={} no longer running)", pid);
            *guard = None;
        }
    }

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

    // Per-mode env var with legacy fallback. e.g. for STATELESS we prefer
    // SIM_NES_DIR_STATELESS, then fall back to SIM_NES_DIR.
    let mode_var = |base: &str| -> Option<String> {
        let suffix = if sim_type == "STATELESS" { "_STATELESS" } else { "_STATEFUL" };
        std::env::var(format!("{}{}", base, suffix))
            .ok()
            .or_else(|| std::env::var(base).ok())
    };

    let sim_bin = match env_required("SIM_BIN") {
        Ok(v) => v,
        Err((s, m)) => { error!("SIM_BIN not set"); return (s, m).into_response(); }
    };
    let sim_nes_dir = match mode_var("SIM_NES_DIR") {
        Some(v) => v,
        None => {
            error!("neither SIM_NES_DIR_{} nor SIM_NES_DIR is set", if sim_type == "STATELESS" { "STATELESS" } else { "STATEFUL" });
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": format!("no NES build dir configured for sim_type={}", sim_type)})),
            )
                .into_response();
        }
    };
    let sim_toml = match mode_var("SIM_TOML") {
        Some(v) => v,
        None => {
            error!("neither SIM_TOML_{} nor SIM_TOML is set", if sim_type == "STATELESS" { "STATELESS" } else { "STATEFUL" });
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": format!("no template TOML configured for sim_type={}", sim_type)})),
            )
                .into_response();
        }
    };
    let sim_output_dir = match env_required("SIM_OUTPUT_DIR") {
        Ok(v) => v,
        Err((s, m)) => { error!("SIM_OUTPUT_DIR not set"); return (s, m).into_response(); }
    };
    let sim_tcp_input_bin = match env_required("SIM_TCP_INPUT_BIN") {
        Ok(v) => v,
        Err((s, m)) => { error!("SIM_TCP_INPUT_BIN not set"); return (s, m).into_response(); }
    };
    let sim_runs = match env_required("SIM_RUNS") {
        Ok(v) => v,
        Err((s, m)) => { error!("SIM_RUNS not set"); return (s, m).into_response(); }
    };

    let live_port = std::env::var("LIVE_LATENCY_PORT").unwrap_or_else(|_| "9001".to_string());
    let latency_sink_host = match env_required("LATENCY_SINK_HOST") {
        Ok(v) => v,
        Err((s, m)) => {
            error!("LATENCY_SINK_HOST not set");
            return (s, m).into_response();
        }
    };
    let latency_sink_port =
        std::env::var("LATENCY_SINK_PORT").unwrap_or_else(|_| "9501".to_string());

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
    let inner = format!("{}_{}", mobile, total);
    let folder = exp_root.join(total.to_string()).join(&inner);
    let folder_str = match folder.to_str() {
        Some(s) => s.to_string(),
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": "folder path is not valid UTF-8"})),
            )
                .into_response();
        }
    };

    // Pre-flight: configuration folder must exist on fat2.
    match ssh_run(&host, &format!("test -d {}", sh_quote(&folder_str))) {
        Ok(out) if out.status.success() => {}
        Ok(_) => {
            warn!("remote folder not found: {}:{}", host, folder_str);
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "ok": false,
                    "error": format!("configuration not found on {}: {}", host, folder_str)
                })),
            )
                .into_response();
        }
        Err(e) => {
            error!("ssh test -d failed: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": format!("ssh test failed: {}", e)})),
            )
                .into_response();
        }
    }

    // Fetch the template TOML from fat2.
    let template = match ssh_run(&host, &format!("cat {}", sh_quote(&sim_toml))) {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).into_owned(),
        Ok(out) => {
            error!(
                "remote cat {} failed: {}",
                sim_toml,
                String::from_utf8_lossy(&out.stderr)
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "ok": false,
                    "error": format!("read SIM_TOML on {}: {}", host, String::from_utf8_lossy(&out.stderr))
                })),
            )
                .into_response();
        }
        Err(e) => {
            error!("ssh cat SIM_TOML failed: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": format!("ssh cat failed: {}", e)})),
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
    let rendered_path = format!("{}/sim_rendered_{}.toml", folder.display(), ts);

    // Upload rendered TOML to fat2 via single-quoted heredoc (no expansion).
    let upload_script = format!(
        "set -e\ncat > {} <<'__SIM_TOML_EOF__'\n{}\n__SIM_TOML_EOF__\n",
        sh_quote(&rendered_path),
        rendered
    );
    match ssh_run(&host, &upload_script) {
        Ok(out) if out.status.success() => {
            info!("rendered TOML uploaded to {}:{}", host, rendered_path);
        }
        Ok(out) => {
            error!(
                "remote upload of rendered TOML failed: status={} stderr={}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "ok": false,
                    "error": format!("upload rendered TOML: {}", String::from_utf8_lossy(&out.stderr))
                })),
            )
                .into_response();
        }
        Err(e) => {
            error!("ssh upload TOML failed: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": format!("ssh upload failed: {}", e)})),
            )
                .into_response();
        }
    }

    // Spawn the local `latency_service` first so its listener is up before
    // the simulator's forwarder tries to dial it. Tear down any prior
    // instance from a previous /start.
    stop_latency_service(&state.latency_child);
    match start_latency_service(&sim_type, &latency_sink_port, &live_port) {
        Ok(child) => {
            let mut lguard = state.latency_child.lock().unwrap();
            *lguard = Some(child);
        }
        Err(e) => {
            error!("could not start latency_service: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "ok": false,
                    "error": format!("start latency_service: {}", e)
                })),
            )
                .into_response();
        }
    }

    info!(
        "spawning simulator on {} via SSH: bin={} type={} nes_dir={} toml={} output={} tcp_input={} runs={} latency_sink={}:{}",
        host, sim_bin, sim_type, sim_nes_dir, rendered_path, sim_output_dir, sim_tcp_input_bin, sim_runs, latency_sink_host, latency_sink_port
    );

    // Spawn the simulator on fat2 with nohup and detach; capture the remote
    // PID by echoing it on stdout.
    let spawn_script = format!(
        r#"set -e
SIM_OUTPUT_DIR={output_dir}
SIM_BIN={sim_bin}
SIM_NES_DIR={sim_nes_dir}
RENDERED={rendered}
TCP_INPUT_BIN={tcp_input_bin}
mkdir -p "$SIM_OUTPUT_DIR"
cd "$(dirname "$SIM_BIN")"
# Raise nofile as high as the kernel/PAM allow. Non-interactive SSH sessions
# often cap below 1048576, so fall through to lower values rather than
# aborting via `set -e`.
ulimit -n 1048576 2>/dev/null \
  || ulimit -n 262144 2>/dev/null \
  || ulimit -n 131072 2>/dev/null \
  || echo "[remote-spawn] WARNING: could not raise nofile" >&2
LATENCY_SINK_HOST={latency_sink_host} \
LATENCY_SINK_PORT={latency_sink_port} \
  nohup "$SIM_BIN" {sim_type} "$SIM_NES_DIR" "$RENDERED" "$SIM_OUTPUT_DIR" "$TCP_INPUT_BIN" {sim_runs} \
  > "$SIM_OUTPUT_DIR/run.log" 2>&1 < /dev/null &
PID=$!
disown 2>/dev/null || true
echo "$PID" > "$SIM_OUTPUT_DIR/run.pid"
echo "$PID"
"#,
        output_dir = sh_quote(&sim_output_dir),
        sim_bin = sh_quote(&sim_bin),
        sim_nes_dir = sh_quote(&sim_nes_dir),
        rendered = sh_quote(&rendered_path),
        tcp_input_bin = sh_quote(&sim_tcp_input_bin),
        latency_sink_host = sh_quote(&latency_sink_host),
        latency_sink_port = sh_quote(&latency_sink_port),
        sim_type = sh_quote(&sim_type),
        sim_runs = sh_quote(&sim_runs),
    );

    match ssh_run(&host, &spawn_script) {
        Ok(out) if out.status.success() => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let pid_str = stdout.lines().last().unwrap_or("").trim();
            match pid_str.parse::<u32>() {
                Ok(pid) => {
                    info!(
                        "simulator spawned on {}, pid={} (log={}:{}/run.log)",
                        host, pid, host, sim_output_dir
                    );
                    *guard = Some(pid);
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "ok": true,
                            "pid": pid,
                            "host": host,
                            "log": format!("{}:{}/run.log", host, sim_output_dir),
                            "pid_file": format!("{}:{}/run.pid", host, sim_output_dir)
                        })),
                    )
                        .into_response()
                }
                Err(e) => {
                    error!(
                        "could not parse PID from remote spawn output: err={} stdout={:?}",
                        e, stdout
                    );
                    stop_latency_service(&state.latency_child);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({
                            "ok": false,
                            "error": format!("parse remote pid: {}", e),
                            "stdout": stdout,
                        })),
                    )
                        .into_response()
                }
            }
        }
        Ok(out) => {
            error!(
                "remote spawn failed: status={} stderr={}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            );
            stop_latency_service(&state.latency_child);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "ok": false,
                    "error": format!("remote spawn failed: {}", String::from_utf8_lossy(&out.stderr))
                })),
            )
                .into_response()
        }
        Err(e) => {
            error!("ssh spawn invocation failed: {}", e);
            stop_latency_service(&state.latency_child);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": format!("ssh invocation failed: {}", e)})),
            )
                .into_response()
        }
    }
}

async fn stop_handler(State(state): State<AppState>) -> impl IntoResponse {
    info!("POST /stop received");

    let host = match env_required("REMOTE_HOST") {
        Ok(v) => v,
        Err((s, m)) => {
            error!("REMOTE_HOST not set");
            return (s, m).into_response();
        }
    };

    let mut guard = state.remote_pid.lock().unwrap();

    if let Some(pid) = *guard {
        info!("sending SIGINT to remote simulator pid={} on {}", pid, host);
        if let Err(e) = ssh_run(&host, &format!("kill -INT {} 2>/dev/null || true", pid)) {
            warn!("ssh kill -INT failed (best-effort): {}", e);
        }

        let mut exited = false;
        for i in 0..50 {
            std::thread::sleep(Duration::from_millis(100));
            if !ssh_remote_running(&host, pid) {
                info!(
                    "remote simulator exited after SIGINT (~{}ms)",
                    (i + 1) * 100
                );
                exited = true;
                break;
            }
        }

        if !exited {
            warn!(
                "remote simulator pid={} did not exit after 5s, sending SIGKILL",
                pid
            );
            if let Err(e) = ssh_run(&host, &format!("kill -9 {} 2>/dev/null || true", pid)) {
                warn!("ssh kill -9 failed: {}", e);
            }
        }
    } else {
        info!("POST /stop: no recorded remote simulator pid");
    }

    *guard = None;

    if let Ok(dir) = std::env::var("SIM_OUTPUT_DIR") {
        let pid_file = format!("{}/run.pid", dir);
        let _ = ssh_run(&host, &format!("rm -f {}", sh_quote(&pid_file)));
    }

    info!("running remote kill chain for orphaned NES processes");
    match ssh_run(&host, KILL_CHAIN) {
        Ok(out) => info!("remote kill chain finished status={}", out.status),
        Err(e) => warn!("remote kill chain failed: {}", e),
    }

    // Tear down the local latency_service so the next /start gets a fresh
    // sink state. SIGINT lets it flush the final bucket and emit EOF.
    stop_latency_service(&state.latency_child);

    info!("POST /stop complete");
    Json(StopResponse { ok: true }).into_response()
}

async fn restart_handler(
    State(state): State<AppState>,
    Json(req): Json<StartReq>,
) -> impl IntoResponse {
    info!("POST /restart received: {:?}", req);
    // Run stop then start; discard the stop response and propagate start's.
    let _ = stop_handler(State(state.clone())).await.into_response();
    start_handler(State(state), Json(req)).await.into_response()
}

async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    info!("GET /status received");

    let host = match std::env::var("REMOTE_HOST") {
        Ok(v) => v,
        Err(_) => {
            warn!("GET /status: REMOTE_HOST not set, reporting not running");
            return Json(StatusResponse {
                running: false,
                pid: None,
            });
        }
    };

    let mut guard = state.remote_pid.lock().unwrap();

    let (running, pid) = match *guard {
        Some(pid) => {
            if ssh_remote_running(&host, pid) {
                (true, Some(pid))
            } else {
                info!("GET /status: remote pid={} no longer running, clearing", pid);
                *guard = None;
                (false, None)
            }
        }
        None => (false, None),
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

    let host = match env_required("REMOTE_HOST") {
        Ok(v) => v,
        Err((s, m)) => return (s, m).into_response(),
    };
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
    let raw = match ssh_run(&host, &format!("cat {}", sh_quote(&log_path))) {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).into_owned(),
        Ok(out) => {
            warn!(
                "GET /deployment: remote cat {} failed: {}",
                log_path,
                String::from_utf8_lossy(&out.stderr)
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("remote read {}: {}", log_path, String::from_utf8_lossy(&out.stderr))
                })),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": format!("ssh cat failed: {}", e)})),
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
        request_ns += cap[1].parse::<u64>().unwrap_or(0);
        placement_ns += cap[2].parse::<u64>().unwrap_or(0);
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
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stdout_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stdout);

    let file_guard = match std::env::var("DAEMON_LOG_DIR") {
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
                warn!("could not create DAEMON_LOG_DIR={}: {} (stdout-only logging)", dir, e);
                None
            }
        },
        Err(_) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(stdout_layer)
                .init();
            warn!("DAEMON_LOG_DIR not set at daemon startup (stdout-only logging)");
            None
        }
    };
    let _file_guard = file_guard;

    let port: u16 = std::env::var("DAEMON_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9000);

    info!("berlin-trams-daemon starting");
    info!("  DAEMON_PORT       = {}", port);
    info!("  REMOTE_HOST       = {:?}", std::env::var("REMOTE_HOST").ok());
    info!("  REMOTE_DIR        = {:?}", std::env::var("REMOTE_DIR").ok());
    info!("  SIM_BIN           = {:?}", std::env::var("SIM_BIN").ok());
    info!("  SIM_TYPE          = {:?}", std::env::var("SIM_TYPE").ok());
    info!("  SIM_NES_DIR       = {:?}", std::env::var("SIM_NES_DIR").ok());
    info!("  SIM_TOML          = {:?}", std::env::var("SIM_TOML").ok());
    info!("  SIM_OUTPUT_DIR    = {:?}", std::env::var("SIM_OUTPUT_DIR").ok());
    info!("  SIM_TCP_INPUT_BIN = {:?}", std::env::var("SIM_TCP_INPUT_BIN").ok());
    info!("  SIM_RUNS          = {:?}", std::env::var("SIM_RUNS").ok());
    info!("  LIVE_LATENCY_PORT   = {:?}", std::env::var("LIVE_LATENCY_PORT").ok());
    info!("  LATENCY_SERVICE_BIN = {:?}", std::env::var("LATENCY_SERVICE_BIN").ok());
    info!("  LATENCY_SINK_HOST   = {:?}", std::env::var("LATENCY_SINK_HOST").ok());
    info!("  LATENCY_SINK_PORT   = {:?}", std::env::var("LATENCY_SINK_PORT").ok());
    info!("  DAEMON_LOG_DIR      = {:?}", std::env::var("DAEMON_LOG_DIR").ok());

    let state = AppState {
        remote_pid: Arc::new(Mutex::new(None)),
        latency_child: Arc::new(Mutex::new(None)),
    };

    let app = Router::new()
        .route("/start", post(start_handler))
        .route("/stop", post(stop_handler))
        .route("/restart", post(restart_handler))
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
