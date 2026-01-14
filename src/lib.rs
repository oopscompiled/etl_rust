pub mod extract;
pub mod model;

pub struct Config {
    pub path_to_data: String,
    pub dry_run: bool,
    pub event_type_filter: Option<String>,
    pub output_file: Option<String>,
}

pub fn run(config: Config) -> Result<(), String> {
    extract::json_lines::check_folder(
        &config.path_to_data,
        config.dry_run,
        config.event_type_filter,
        config.output_file,
    )
}
