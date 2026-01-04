use std::time::Instant;
mod model;
mod extract {
    pub mod json_lines;
}
use crate::extract::json_lines::check_folder;

fn main() {
    let path_to_data = "/Users/pacuk/etl_data";
    let start = Instant::now();

    if let Err(e) = check_folder(path_to_data) {
        eprintln!("Fatal error: {}", e);
    }

    let elapsed = start.elapsed();
    println!("⏱️ Time: {:.2?}", elapsed);
}
