# Live latency streaming — first plan

## Goal

Replace the current Avro-file output path with a live, pre-aggregated stream of
end-to-end latency suitable for driving a real-time graph. File output is
removed for now; deployment-latency streaming is deferred.

## Inputs (unchanged, from NES TCP sink)

- Stateful `OutputTuple` (14 × u64, 112 B) — `output.rs:20`.
- Stateless `OutputTupleStateless` (5 × u64, 40 B) — `output.rs:84`.

## Latency formulas

- Stateless: `lat = emission_time - event_time`.
- Stateful (join): `lat = emission_time_1 - max(event_time_1, event_time_2)`
  (time from the last source event in the join pair until emission).

Times are NES-internal u64 (nanoseconds since some epoch). We treat them as
nanoseconds and convert to milliseconds for the wire format.

## Bucketing

- **Bucket size: 100 ms**, fixed for now (`const BUCKET_MS: u64 = 100`).
- Bucket key = `floor(emission_time_ns / 100_000_000) * 100_000_000`.
  We bucket by *event-time* (`emission_time` as reported by NES), not wall
  clock — this matches how an offline notebook would group it and survives
  out-of-order arrival within the polling tick.
- Per bucket we keep: `count`, `sum_lat_ns`, `min`, `max`, and an
  `hdrhistogram::Histogram<u64>` (1 µs–60 s range, 3 sig figs ≈ a few KB) for
  p50/p95/p99.
- A bucket is *flushed* once we observe a tuple whose bucket key is greater,
  AND once at end-of-stream for any still-open buckets.
  - Out-of-order tuples that fall into an already-flushed bucket are dropped
    with a `late_tuples` counter (logged, not streamed) — keeps the wire
    monotonic in `t_ms`.

## Wire format

Line-delimited JSON over TCP. One frame per closed bucket:

```json
{"t_ms": 1714300000000, "count": 1234, "min": 1.2, "p50": 12.4, "p95": 38.9, "p99": 71.2, "max": 102.0, "mean": 17.3}
```

`t_ms` is the bucket *start* in ms. Latency fields in ms (f64).

Socket address is left unconfigured per request — the sink will be constructed
with a placeholder (e.g. `127.0.0.1:0`) and a `TODO` marker; until that's
filled in, the sink will log frames to stdout instead of writing to TCP.
A single bool flag in `LiveLatencySink::new` toggles stdout-vs-tcp without
touching call sites.

## Code changes

### New module: `src/live_latency.rs`

```rust
pub struct LiveLatencySink {
    sink_kind: SinkKind,            // Stdout for now, Tcp(BufWriter<TcpStream>) later
    current: Option<BucketState>,   // None until first tuple
    bucket_ns: u64,                 // 100_000_000
    late_tuples: u64,
    // any per-experiment metadata we want in the frame (run id, etc.) — TBD
}

struct BucketState {
    start_ns: u64,
    count: u64,
    sum_ns: u64,
    min_ns: u64,
    max_ns: u64,
    hist: hdrhistogram::Histogram<u64>,
}

impl LiveLatencySink {
    pub fn new_stdout() -> Self { ... }
    pub fn record_stateful(&mut self, t: &OutputTuple) { ... }
    pub fn record_stateless(&mut self, t: &OutputTupleStateless) { ... }
    pub fn flush(&mut self) -> Result<(), Box<dyn Error>> { ... }   // flush open bucket at end
    fn maybe_emit(&mut self, tuple_bucket_start_ns: u64) -> io::Result<()> { ... }
    fn emit_frame(&mut self, b: &BucketState) -> io::Result<()> { ... }
}
```

Add `hdrhistogram = "7"` to `Cargo.toml`.

### `src/output.rs`

- Replace `OutputBundle` (`Stateful`/`Stateless` `AvroOutputWriter` variants)
  with a single `LiveLatencySink` shared via `Arc<Mutex<…>>`. Drop
  `AvroOutputWriter` / `AvroOutputWriterStateless` (or keep behind a feature
  flag if you want to revert easily — confirm).
- Rewrite `handle_connection` so parsing happens **inside** the polling loop
  rather than after it:
  - On each `try_read_buf`, drain any *complete* tuples from the head of `buf`,
    parse them, call `sink.record_*`, retain partial trailing bytes for the
    next iteration.
  - Keep the existing exit conditions (shutdown, line count, timeout) and the
    sanity asserts for stateful AVRO mode.
  - Lock contention: each connection thread locks the shared sink per tuple
    batch (per `try_read_buf`), not per tuple, to keep the mutex cheap.
- After loop: call `sink.lock().flush()` to drain the open bucket. (The old
  Avro `flush` logic is gone.)

### `src/main.rs`

- Drop the `File::create(file_path)` and `OutputBundle::Stateful/Stateless`
  construction (`main.rs:189–200`).
- Construct `Arc::new(Mutex::new(LiveLatencySink::new_stdout()))` instead.
- Pass it through to `handle_connection` (signature change: `Arc<Mutex<LiveLatencySink>>`).
- Keep the `tuple_count.csv` and `reconnects.csv` writes for now — they're
  small and orthogonal.
- Skip `create_notebook(...)` invocation when running in live mode (it expects
  an Avro file that no longer exists). Either gate on the sink type or
  unconditionally skip until live mode becomes a config flag.

### `src/config.rs`

No changes for this first pass — live mode is hardcoded as the default since
the user asked to *replace* file output. A `OutputType::Live { addr }` variant
can be added when the socket address is known.

## What I'm explicitly not doing this pass

- Deployment-latency streaming (user is rethinking the source).
- Real TCP socket connect — placeholder/stdout only.
- Config knob for bucket size — hardcoded 100 ms.
- Keeping the Avro file path alongside the socket — replace, not parallel.
- Per-source breakdown (single aggregate stream).

## Open questions before coding

1. OK to delete `AvroOutputWriter` / `AvroOutputWriterStateless` outright, or
   keep them dead-code behind `#[allow(dead_code)]` for easy revert?
2. OK to bucket by `emission_time` (NES event time) rather than wall clock?
   Matters if NES emission timestamps and host wall clock diverge — for
   replaying experiments, event time is usually what you want on the x-axis.
3. Are p50/p95/p99 the right summary, or do you want raw `(t_ms, count, mean,
   max)` only? Affects whether we pull in `hdrhistogram` at all.
4. Is stdout-as-placeholder fine until the socket address is decided, or
   would you rather the sink no-op silently?
