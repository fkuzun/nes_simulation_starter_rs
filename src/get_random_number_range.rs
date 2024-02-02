use rand::Rng;

fn main() {

    let mut rng = rand::thread_rng();
    let min_id = 13;
    let max_id = 110;
    let mut numbers: Vec<u64> = vec![];
    for _i in 0..10 {
        let mut number = rng.gen_range(min_id..max_id);
        while numbers.contains(&number) {
            number = rng.gen_range(min_id..max_id);
        }
        numbers.push(number);
    }
    println!("{:?}", numbers);
}
