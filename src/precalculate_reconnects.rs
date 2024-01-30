use std::env;
use std::time::Duration;
use rand::Rng;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 6 {
        eprintln!("Usage: {} <min_id> <max_id> <output_file> <experiment_duration> <interval>", args[0]);
        std::process::exit(1);
    }
    let min_id = args[1].parse::<usize>().expect("Invalid duration");
    //let min_id = 12;
    let max_id = args[2].parse::<usize>().expect("Invalid duration");
    //let max_id = 110;
    let output_file = &args[3][..];
    let duration = Duration::from_millis(args[4].parse::<u64>().expect("Invalid duration"));
    let interval = Duration::from_millis(args[5].parse::<u64>().expect("Invalid interval"));

    // let json_string = std::fs::read_to_string(&self.paths.fixed_topology_nodes)?;
    // let topology: FixedTopology = serde_json::from_str(json_string.as_str())?;

    let mut timestamp = Duration::new(0, 0);
    let mut csv_writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_path(&output_file).unwrap();
    let mut rng = rand::thread_rng();
    while timestamp < duration {
        csv_writer.serialize((rng.gen_range(min_id..max_id), timestamp.as_nanos()))?;
        timestamp += interval;
    }
    Ok(())

}