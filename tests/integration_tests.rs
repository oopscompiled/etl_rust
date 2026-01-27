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
            quiet_mode: false,
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
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_single_json_file() {
        let tmp_dir = tempdir().unwrap();
        fs::write(
            tmp_dir.path().join("1.json"),
            r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#,
        )
        .unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_multiple_json_files() {
        let tmp_dir = tempdir().unwrap();
        let event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;

        for i in 1..=4 {
            fs::write(tmp_dir.path().join(format!("file-{}.json", i)), event).unwrap();
        }

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_multiple_events_per_file() {
        let tmp_dir = tempdir().unwrap();
        let events = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}
{"type":"PullRequestEvent","actor":{"login":"user2"},"repo":{"name":"repo2"},"created_at":"2024-01-02T00:00:00Z"}
{"type":"IssuesEvent","actor":{"login":"user3"},"repo":{"name":"repo3"},"created_at":"2024-01-03T00:00:00Z"}"#;

        fs::write(tmp_dir.path().join("events.json"), events).unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
            quiet_mode: false,
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
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_mixed_valid_invalid_json() {
        let tmp_dir = tempdir().unwrap();
        let valid_event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;
        let invalid_json = r#"{"incomplete": "#;

        let content = format!("{}\n{}\n{}", valid_event, invalid_json, valid_event);
        fs::write(tmp_dir.path().join("mixed.json"), content).unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_dry_run_mode() {
        let tmp_dir = tempdir().unwrap();
        let event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;

        for i in 1..=2 {
            fs::write(tmp_dir.path().join(format!("dry-{}.json", i)), event).unwrap();
        }

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: true,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn dry_run_does_not_create_output_file() {
        let tmp_dir = tempdir().unwrap();
        let event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), event).unwrap();

        let output_file = tmp_dir.path().join("output.json");
        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: true,
            event_type_filter: None,
            output_file: Some(output_file.to_str().unwrap().to_string()),
            show_stats: false,
            quiet_mode: false,
        };

        run(config).unwrap();

        assert!(output_file.exists());
    }

    #[test]
    fn run_with_valid_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        let event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), event).unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: Some("PushEvent".to_string()),
            output_file: None,
            show_stats: false,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_invalid_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        let event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), event).unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: Some("InvalidEventType".to_string()),
            output_file: None,
            show_stats: false,
            quiet_mode: false,
        };
        let err = run(config).unwrap_err();
        assert!(err.contains("Invalid event type"));
    }

    #[test]
    fn run_with_multiple_event_types_filters() {
        let tmp_dir = tempdir().unwrap();
        let events = vec![
            r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#,
            r#"{"type":"PullRequestEvent","actor":{"login":"user2"},"repo":{"name":"repo2"},"created_at":"2024-01-02T00:00:00Z"}"#,
            r#"{"type":"IssuesEvent","actor":{"login":"user3"},"repo":{"name":"repo3"},"created_at":"2024-01-03T00:00:00Z"}"#,
        ];

        let content = events.join("\n");
        fs::write(tmp_dir.path().join("events.json"), content).unwrap();

        // Фильтруем только PushEvent
        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: Some("PushEvent".to_string()),
            output_file: None,
            show_stats: false,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_output_file() {
        let tmp_dir = tempdir().unwrap();
        let event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), event).unwrap();

        let output_file = tmp_dir.path().join("output.json");
        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: Some(output_file.to_str().unwrap().to_string()),
            show_stats: false,
            quiet_mode: false,
        };

        assert!(run(config).is_ok());
        assert!(output_file.exists());
    }

    #[test]
    fn run_with_output_file_invalid_path() {
        let tmp_dir = tempdir().unwrap();
        let event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), event).unwrap();

        let output_file = "/invalid/path/output.json";
        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: Some(output_file.to_string()),
            show_stats: false,
            quiet_mode: false,
        };

        let result = run(config);
        assert!(result.is_err());
    }

    #[test]
    fn run_with_show_stats() {
        let tmp_dir = tempdir().unwrap();
        let events = vec![
            r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#,
            r#"{"type":"PushEvent","actor":{"login":"user2"},"repo":{"name":"repo2"},"created_at":"2024-01-02T00:00:00Z"}"#,
        ];

        let content = events.join("\n");
        fs::write(tmp_dir.path().join("events.json"), content).unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: true,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_show_stats_empty_events() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("bad.json"), r#"{"invalid": "event"}"#).unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: true,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_quiet_mode() {
        let tmp_dir = tempdir().unwrap();
        let event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), event).unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
            quiet_mode: true,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_dry_run_with_quiet_mode() {
        let tmp_dir = tempdir().unwrap();
        let event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), event).unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: true,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
            quiet_mode: true,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_all_options_combined() {
        let tmp_dir = tempdir().unwrap();
        let events = vec![
            r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#,
            r#"{"type":"PullRequestEvent","actor":{"login":"user2"},"repo":{"name":"repo2"},"created_at":"2024-01-02T00:00:00Z"}"#,
        ];

        let content = events.join("\n");
        fs::write(tmp_dir.path().join("events.json"), content).unwrap();

        let output_file = tmp_dir.path().join("output.json");
        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: Some("PushEvent".to_string()),
            output_file: Some(output_file.to_str().unwrap().to_string()),
            show_stats: true,
            quiet_mode: false,
        };

        assert!(run(config).is_ok());
        assert!(output_file.exists());
    }

    #[test]
    fn run_with_empty_lines_in_json_file() {
        let tmp_dir = tempdir().unwrap();
        let content = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}

{"type":"PullRequestEvent","actor":{"login":"user2"},"repo":{"name":"repo2"},"created_at":"2024-01-02T00:00:00Z"}

"#;
        fs::write(tmp_dir.path().join("events.json"), content).unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_many_files() {
        let tmp_dir = tempdir().unwrap();
        let event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;

        for i in 1..=10 {
            fs::write(tmp_dir.path().join(format!("file-{:02}.json", i)), event).unwrap();
        }

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_non_json_files_in_folder() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("readme.txt"), "This is not JSON").unwrap();
        fs::write(tmp_dir.path().join("data.csv"), "col1,col2\n1,2").unwrap();
        let event = r#"{"type":"PushEvent","actor":{"login":"user"},"repo":{"name":"repo"},"created_at":"2024-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), event).unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }

    #[test]
    fn run_with_very_large_event() {
        let tmp_dir = tempdir().unwrap();
        let large_payload = "x".repeat(10000);
        let event = format!(
            r#"{{"type":"PushEvent","actor":{{"login":"user"}},"repo":{{"name":"repo"}},"created_at":"2024-01-01T00:00:00Z","payload":"{}"}}"#,
            large_payload
        );
        fs::write(tmp_dir.path().join("large.json"), event).unwrap();

        let config = Config {
            path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
            dry_run: false,
            event_type_filter: None,
            output_file: None,
            show_stats: false,
            quiet_mode: false,
        };
        assert!(run(config).is_ok());
    }
}
