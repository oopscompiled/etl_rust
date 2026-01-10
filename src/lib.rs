pub mod extract;
pub mod model;

pub struct Config {
    pub path_to_data: String,
    pub dry_run: bool,
}

pub fn run(config: Config) -> Result<(), String> {
    extract::json_lines::check_folder(&config.path_to_data, config.dry_run)
}
