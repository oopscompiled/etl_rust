use clap::Parser;
use etl_rust::{Config, run};
use std::time::Instant;

#[derive(Parser)]
#[command(version, about = "ETL tool for processing JSON lines")]
struct Cli {
    #[arg(short, long)]
    path: String,
}

fn main() {
    // 1. Парсинг аргументов
    let cli = Cli::parse();
    println!("PATH = {:?}", cli.path);

    // 2. Превращение аргументов CLI в конфигурацию библиотеки
    let config = Config {
        path_to_data: cli.path,
    };

    let start = Instant::now();

    // 3. Запуск логики
    if let Err(e) = run(config) {
        eprintln!("Fatal error: {}", e);
        std::process::exit(1);
    }

    println!("⏱️ Time: {:.2?}", start.elapsed());
}
