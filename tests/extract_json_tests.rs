#[cfg(test)]
mod tests {
    use etl_rust::extract::json_lines::{check_folder, receive_all};
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_check_folder_with_invalid_path() {
        let result = check_folder("/non/existent/path", false, None, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unable to read folder"));
    }

    #[test]
    fn test_check_folder_with_empty_folder() {
        let tmp_dir = tempdir().unwrap();
        let result = check_folder(tmp_dir.path().to_str().unwrap(), false, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_single_json_file() {
        let tmp_dir = tempdir().unwrap();
        fs::write(
            tmp_dir.path().join("1.json"),
            r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#,
        )
        .unwrap();

        let result = check_folder(tmp_dir.path().to_str().unwrap(), false, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_multiple_json_files() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;

        for i in 1..=4 {
            fs::write(tmp_dir.path().join(format!("file-{}.json", i)), json_data).unwrap();
        }

        let result = check_folder(tmp_dir.path().to_str().unwrap(), false, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_invalid_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), "{}").unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            Some("InvalidEventType".to_string()),
            None,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid event type"));
    }

    #[test]
    fn test_check_folder_with_valid_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), json_data).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            Some("PushEvent".to_string()),
            None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_dry_run_mode() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;

        for i in 1..=3 {
            fs::write(tmp_dir.path().join(format!("dry-{}.json", i)), json_data).unwrap();
        }

        let result = check_folder(tmp_dir.path().to_str().unwrap(), true, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_ignores_non_json_files() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("file.txt"), "some text").unwrap();
        fs::write(tmp_dir.path().join("file.yaml"), "key: value").unwrap();

        let result = check_folder(tmp_dir.path().to_str().unwrap(), false, None, None);
        assert!(result.is_ok());
    }

    // Tests for receive_all function
    #[test]
    fn test_receive_all_with_valid_json_file() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, json_data).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_receive_all_with_multiple_lines() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}
{"id":"124","type":"PullRequestEvent","actor":{"id":2,"login":"user2","gravatar_id":"","url":"","avatar_url":"","display_login":"user2"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-02T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, json_data).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_receive_all_with_empty_lines() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}

{"id":"124","type":"PullRequestEvent","actor":{"id":2,"login":"user2","gravatar_id":"","url":"","avatar_url":"","display_login":"user2"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-02T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, json_data).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_receive_all_with_invalid_json_line() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}
{"invalid json":
{"id":"124","type":"PullRequestEvent","actor":{"id":2,"login":"user2","gravatar_id":"","url":"","avatar_url":"","display_login":"user2"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-02T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, json_data).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        // Should succeed but skip the invalid line
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_receive_all_with_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        let push_event = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        let pr_event = r#"{"id":"124","type":"PullRequestEvent","actor":{"id":2,"login":"user2","gravatar_id":"","url":"","avatar_url":"","display_login":"user2"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-02T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, format!("{}\n{}", push_event, pr_event)).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), Some("PushEvent".to_string()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_receive_all_with_non_matching_filter() {
        let tmp_dir = tempdir().unwrap();
        let push_event = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, push_event).unwrap();

        let result = receive_all(
            file_path.to_str().unwrap(),
            Some("PullRequestEvent".to_string()),
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_receive_all_with_nonexistent_file() {
        let result = receive_all("/nonexistent/file.json", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_receive_all_with_empty_file() {
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("empty.json");
        fs::write(&file_path, "").unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_receive_all_with_only_whitespace_and_empty_lines() {
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("whitespace.json");
        fs::write(&file_path, "\n  \n\t\n").unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }
}

#[cfg(test)]
mod save_tests {
    use etl_rust::extract::json_lines::check_folder;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_save_events_to_output_file() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), json_data).unwrap();

        let output_file = tmp_dir.path().join("output.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            None,
            Some(output_file.to_str().unwrap().to_string()),
        );

        assert!(result.is_ok());
        assert!(output_file.exists());
        let content = fs::read_to_string(&output_file).unwrap();
        assert!(!content.is_empty());
        assert!(content.contains("PushEvent"));
    }

    #[test]
    fn test_save_filtered_events_only() {
        let tmp_dir = tempdir().unwrap();
        let push_event = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        let pr_event = r#"{"id":"124","type":"PullRequestEvent","actor":{"id":2,"login":"user2","gravatar_id":"","url":"","avatar_url":"","display_login":"user2"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-02T00:00:00Z"}"#;

        fs::write(
            tmp_dir.path().join("1.json"),
            format!("{}\n{}", push_event, pr_event),
        )
        .unwrap();

        let output_file = tmp_dir.path().join("push_only.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            Some("PushEvent".to_string()),
            Some(output_file.to_str().unwrap().to_string()),
        );

        assert!(result.is_ok());
        let content = fs::read_to_string(&output_file).unwrap();
        assert!(content.contains("PushEvent"));
        assert!(!content.contains("PullRequestEvent"));
    }

    #[test]
    fn test_save_overwrites_existing_file() {
        let tmp_dir = tempdir().unwrap();
        let output_file = tmp_dir.path().join("output.jsonl");

        // Create a file with initial content
        fs::write(&output_file, "old content").unwrap();
        assert_eq!(fs::read_to_string(&output_file).unwrap(), "old content");

        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), json_data).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            None,
            Some(output_file.to_str().unwrap().to_string()),
        );

        assert!(result.is_ok());
        let content = fs::read_to_string(&output_file).unwrap();
        assert!(!content.contains("old content"));
        assert!(content.contains("PushEvent"));
    }

    #[test]
    fn test_save_multiple_events_jsonl_format() {
        let tmp_dir = tempdir().unwrap();
        let push_event = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        let create_event = r#"{"id":"125","type":"CreateEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-03T00:00:00Z"}"#;

        fs::write(
            tmp_dir.path().join("1.json"),
            format!("{}\n{}", push_event, create_event),
        )
        .unwrap();

        let output_file = tmp_dir.path().join("output.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            None,
            Some(output_file.to_str().unwrap().to_string()),
        );

        assert!(result.is_ok());
        let content = fs::read_to_string(&output_file).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("PushEvent"));
        assert!(lines[1].contains("CreateEvent"));
    }

    #[test]
    fn test_save_without_output_file() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), json_data).unwrap();

        // Should work fine without output file being specified
        let result = check_folder(tmp_dir.path().to_str().unwrap(), false, None, None);

        assert!(result.is_ok());
    }

    #[test]
    fn test_save_with_invalid_output_path() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), json_data).unwrap();

        // Try to save to a non-existent directory
        let invalid_output = "/non/existent/dir/output.jsonl";
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            None,
            Some(invalid_output.to_string()),
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_save_empty_filter_result() {
        let tmp_dir = tempdir().unwrap();
        let push_event = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;
        fs::write(tmp_dir.path().join("1.json"), push_event).unwrap();

        let output_file = tmp_dir.path().join("output.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            Some("PullRequestEvent".to_string()),
            Some(output_file.to_str().unwrap().to_string()),
        );

        assert!(result.is_ok());
        // Output file should exist but be empty (or contain only whitespace)
        assert!(output_file.exists());
        let content = fs::read_to_string(&output_file).unwrap();
        assert_eq!(content.trim().len(), 0);
    }

    #[test]
    fn test_save_multiple_files() {
        let tmp_dir = tempdir().unwrap();
        let json_data = r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#;

        for i in 1..=3 {
            fs::write(tmp_dir.path().join(format!("file-{}.json", i)), json_data).unwrap();
        }

        let output_file = tmp_dir.path().join("output.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            None,
            Some(output_file.to_str().unwrap().to_string()),
        );

        assert!(result.is_ok());
        let content = fs::read_to_string(&output_file).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 3);
    }
}
