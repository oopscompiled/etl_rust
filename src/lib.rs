pub mod extract;
pub mod model;

pub struct Config {
    pub path_to_data: String,
    pub dry_run: bool,
    pub show_stats: bool,
    pub event_type_filter: Option<String>,
    pub output_file: Option<String>,
    pub quiet_mode: bool,
}

pub fn run(config: Config) -> Result<(), String> {
    crate::extract::json_lines::check_folder(
        &config.path_to_data,
        config.dry_run,
        config.show_stats,
        config.event_type_filter,
        config.output_file,
        config.quiet_mode,
    )
}
