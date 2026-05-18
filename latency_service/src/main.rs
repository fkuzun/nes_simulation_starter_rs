//! Standalone latency-aggregation service. Listens for binary NES output
//! tuples forwarded from fat-2, buckets them into 1 ms windows via
//! `LiveLatencySink`, and broadcasts the resulting JSON frames to any TCP
//! subscriber (the bridge in particular).
//!
//! Two listeners:
//! - `LATENCY_SINK_PORT` (default 9501): inbound TCP from the simulator's
//!   forwarder on fat-2. Each NES connection becomes one inbound stream here.
//! - `LIVE_LATENCY_PORT` (default 9001): outbound TCP to bridge subscribers.
//!
//! The daemon kills and respawns this process for every `/start`, so we do
//! not need a `/reset` endpoint — fresh state per run is implicit.

use simulation_runner_lib::config::ExperimentType;
use simulation_runner_lib::live_latency::LiveLatencySink;
use simulation_runner_lib::output::{OutputTuple, OutputTupleStateless};

use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let sim_type_str = env::var("SIM_TYPE").unwrap_or_else(|_| "STATEFUL".to_string());
    let experiment_type: ExperimentType =
        serde_json::from_str(&format!("\"{}\"", sim_type_str)).unwrap_or_else(|e| {
            eprintln!(
                "[latency_service] could not parse SIM_TYPE={:?}: {}; defaulting to STATEFUL",
                sim_type_str, e
            );
            ExperimentType::STATEFUL
        });
    let sink_port: u16 = env::var("LATENCY_SINK_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9501);
    let live_port: u16 = env::var("LIVE_LATENCY_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9001);

    eprintln!(
        "[latency_service] starting: sim_type={:?} LATENCY_SINK_PORT={} LIVE_LATENCY_PORT={}",
        experiment_type, sink_port, live_port
    );

    let (live_tx, _) = broadcast::channel::<String>(1024);
    let sink = Arc::new(Mutex::new(LiveLatencySink::new_tcp(live_tx.clone())));

    // Graceful shutdown: on SIGINT, flush the sink so the final bucket lands,
    // emit the EOF marker so the bridge stops reconnecting, then exit.
    {
        let sink = sink.clone();
        let live_tx = live_tx.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!("[latency_service] SIGINT received, flushing and emitting EOF");
                if let Ok(mut s) = sink.lock() {
                    if let Err(e) = s.flush() {
                        eprintln!("[latency_service] flush failed: {}", e);
                    }
                }
                let _ = live_tx.send("{\"eof\":true}\n".to_string());
                std::process::exit(0);
            }
        });
    }

    spawn_live_listener(live_tx.clone(), live_port);
    run_sink_listener(sink, experiment_type, sink_port).await
}

fn spawn_live_listener(live_tx: broadcast::Sender<String>, live_port: u16) {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(format!("0.0.0.0:{}", live_port)).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!(
                    "[live_latency] bind 0.0.0.0:{} failed: {}",
                    live_port, e
                );
                return;
            }
        };
        eprintln!("[live_latency] TCP listener on 0.0.0.0:{}", live_port);
        loop {
            let (mut sock, peer) = match listener.accept().await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("[live_latency] accept error: {}", e);
                    continue;
                }
            };
            eprintln!("[live_latency] client connected: {}", peer);
            let mut rx = live_tx.subscribe();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(line) => {
                            if sock.write_all(line.as_bytes()).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            eprintln!(
                                "[live_latency] client {} lagged by {} frames",
                                peer, n
                            );
                        }
                        Err(_) => break,
                    }
                }
                eprintln!("[live_latency] client disconnected: {}", peer);
            });
        }
    });
}

async fn run_sink_listener(
    sink: Arc<Mutex<LiveLatencySink>>,
    experiment_type: ExperimentType,
    sink_port: u16,
) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", sink_port)).await?;
    eprintln!(
        "[latency_service] tuple listener on 0.0.0.0:{}",
        sink_port
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        eprintln!("[latency_service] forwarder connected: {}", peer);
        let sink = sink.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_tuple_stream(stream, sink, experiment_type).await {
                eprintln!("[latency_service] {} stream error: {}", peer, e);
            }
            eprintln!("[latency_service] forwarder disconnected: {}", peer);
        });
    }
}

async fn handle_tuple_stream(
    mut stream: TcpStream,
    sink: Arc<Mutex<LiveLatencySink>>,
    experiment_type: ExperimentType,
) -> Result<(), Box<dyn Error>> {
    let tuple_size = if experiment_type.is_stateful() {
        std::mem::size_of::<OutputTuple>()
    } else {
        std::mem::size_of::<OutputTupleStateless>()
    };
    let mut buf: Vec<u8> = Vec::with_capacity(1 << 20);
    let mut consumed: usize = 0;
    let mut chunk = vec![0u8; 65536];

    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);

        let available = buf.len() - consumed;
        let complete = available / tuple_size;
        if complete > 0 {
            let mut sink_lock = sink.lock().expect("LiveLatencySink mutex poisoned");
            for _ in 0..complete {
                let slice = &buf[consumed..consumed + tuple_size];
                consumed += tuple_size;
                if experiment_type.is_stateful() {
                    let t = OutputTuple::from_bytes(slice);
                    sink_lock.record_stateful(&t)?;
                } else {
                    let t = OutputTupleStateless::from_bytes(slice);
                    sink_lock.record_stateless(&t)?;
                }
            }
            if consumed >= 1 << 20 {
                buf.drain(..consumed);
                consumed = 0;
            }
        }
    }
    Ok(())
}
