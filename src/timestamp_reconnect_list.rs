use std::time::Duration;
use simulation_runner_lib::PrecalculatedReconnect;

fn main() {

    let input_precalculated_reconnects = "/home/x/uni/ba/experiments/nes_experiment_input/1_mobile_worker_explicit_reconnects_to_non_source_hosting_nodes/reconnects.csv";
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(&input_precalculated_reconnects).unwrap();
    let reconnects: Vec<PrecalculatedReconnect> = rdr.deserialize().collect::<Result<_, csv::Error>>().unwrap();

    let output_precalc_path = "/home/x/uni/ba/experiments/nes_experiment_input/1_mobile_worker_explicit_reconnects_to_non_source_hosting_nodes_1_sec_interval/reconnects.csv";
    let mut csv_writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(&output_precalc_path).unwrap();


    let mut timestamp = Duration::new(0, 0);
    let mut interval = Duration::new(1, 0);

    for mut reconnect in reconnects {
        reconnect.offset = timestamp;
        csv_writer.serialize(reconnect).unwrap();
        timestamp += interval;
    }
    csv_writer.flush().unwrap();
}