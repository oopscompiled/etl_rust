use etl_rust::{Config, run};

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn run_with_invalid_path() {
        let config = Config {
            path_to_data: "/non/existent/path".into(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
        };
        let err = run(config).unwrap_err();
        assert!(err.contains("Unable to read folder"));
    }

    #[test]
    fn run_with_empty_folder() {
        let tmp_dir = tempdir().unwrap();
        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_single_json_file() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), r#"{"type":"PushEvent"}"#).unwrap();
        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_multiple_json_files() {
        let tmp_dir = tempdir().unwrap();
        for i in 1..=4 {
            fs::write(
                tmp_dir.path().join(format!("file-{}.json", i)),
                r#"{"type":"PushEvent"}"#,
            )
            .unwrap();
        }
        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_invalid_json() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("bad.json"), r#"{"type": }"#).unwrap();
        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_dry_run_like() {
        let tmp_dir = tempdir().unwrap();
        for i in 1..=2 {
            fs::write(
                tmp_dir.path().join(format!("dry-{}.json", i)),
                r#"{"type":"PushEvent"}"#,
            )
            .unwrap();
        }
        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), r#"{"type":"PushEvent"}"#).unwrap();
        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: Some("PushEvent".to_string()),
            output_file: None,
            show_stats: false,
        };
        assert!(run(config).is_ok());
    }
}
