// main.rs
use clap::Parser;
use etl_rust::{Config, run};
use std::time::Instant;

#[derive(Parser)]
#[command(version, about = "ETL tool for processing JSON lines")]
struct Cli {
    #[arg(short, long)]
    path: String,

    #[arg(short, long)]
    show_time: bool,

    #[arg(long)]
    dry_run: bool,

    #[arg(long, help = "Show event type analytics")]
    stats: bool,

    #[arg(
        long,
        help = "Filter by event type (e.g., PushEvent, PullRequestEvent)"
    )]
    event_type: Option<String>,

    #[arg(short, long, help = "Output file path for results")]
    output: Option<String>,

    #[arg(long, help = "Quiet Mode (suppressing output)")]
    quiet: bool,
}

fn main() {
    let cli = Cli::parse();
    let config = Config {
        path_to_data: cli.path,
        dry_run: cli.dry_run,
        show_stats: cli.stats,
        event_type_filter: cli.event_type,
        output_file: cli.output,
        quiet_mode: cli.quiet,
    };

    let start = Instant::now();
    if let Err(e) = run(config) {
        eprintln!("Fatal error: {}", e);
        std::process::exit(1);
    }
    if cli.show_time {
        println!("⏱️ Time: {:.2?}", start.elapsed());
    }
}
