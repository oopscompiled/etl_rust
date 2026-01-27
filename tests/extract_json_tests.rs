#[cfg(test)]
mod tests {
    use etl_rust::extract::json_lines::{check_folder, receive_all};
    use std::fs;
    use tempfile::tempdir;

    // Helper function to get valid JSON event
    fn valid_event() -> &'static str {
        r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#
    }

    fn pr_event() -> &'static str {
        r#"{"id":"124","type":"PullRequestEvent","actor":{"id":2,"login":"user2","gravatar_id":"","url":"","avatar_url":"","display_login":"user2"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-02T00:00:00Z"}"#
    }

    fn create_event() -> &'static str {
        r#"{"id":"125","type":"CreateEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-03T00:00:00Z"}"#
    }

    #[test]
    fn test_check_folder_with_invalid_path() {
        let result = check_folder("/non/existent/path", false, false, None, None, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unable to read folder"));
    }

    #[test]
    fn test_check_folder_with_empty_folder() {
        let tmp_dir = tempdir().unwrap();
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_single_json_file() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_multiple_json_files() {
        let tmp_dir = tempdir().unwrap();

        for i in 1..=4 {
            fs::write(
                tmp_dir.path().join(format!("file-{}.json", i)),
                valid_event(),
            )
            .unwrap();
        }

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_multiple_events_per_file() {
        let tmp_dir = tempdir().unwrap();
        let content = format!("{}\n{}\n{}", valid_event(), pr_event(), create_event());
        fs::write(tmp_dir.path().join("events.json"), content).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_invalid_json() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("bad.json"), r#"{"type": }"#).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_mixed_valid_invalid_json() {
        let tmp_dir = tempdir().unwrap();
        let invalid_json = r#"{"incomplete": "#;
        let content = format!("{}\n{}\n{}", valid_event(), invalid_json, valid_event());
        fs::write(tmp_dir.path().join("mixed.json"), content).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_dry_run_mode() {
        let tmp_dir = tempdir().unwrap();
        for i in 1..=3 {
            fs::write(
                tmp_dir.path().join(format!("dry-{}.json", i)),
                valid_event(),
            )
            .unwrap();
        }

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            true,
            false,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_dry_run_does_not_process_events() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            true,
            false,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_invalid_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), "{}").unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            Some("InvalidEventType".to_string()),
            None,
            false,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid event type"));
    }

    #[test]
    fn test_check_folder_with_valid_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            Some("PushEvent".to_string()),
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_multiple_event_types_filters() {
        let tmp_dir = tempdir().unwrap();
        let content = format!("{}\n{}\n{}", valid_event(), pr_event(), create_event());
        fs::write(tmp_dir.path().join("events.json"), content).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            Some("PushEvent".to_string()),
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_filter_no_matching_events() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            Some("IssuesEvent".to_string()),
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_ignores_non_json_files() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("file.txt"), "some text").unwrap();
        fs::write(tmp_dir.path().join("file.yaml"), "key: value").unwrap();
        fs::write(tmp_dir.path().join("readme.md"), "# README").unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_non_json_and_json_files() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("file.txt"), "some text").unwrap();
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_quiet_mode() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            None,
            true,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_dry_run_with_quiet_mode() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            true,
            false,
            None,
            None,
            true,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_show_stats() {
        let tmp_dir = tempdir().unwrap();
        let content = format!("{}\n{}", valid_event(), valid_event());
        fs::write(tmp_dir.path().join("events.json"), content).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            true,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_folder_with_show_stats_empty_events() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("bad.json"), r#"{"invalid": "event"}"#).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            true,
            None,
            None,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_receive_all_with_valid_json_file() {
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, valid_event()).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_receive_all_with_multiple_lines() {
        let tmp_dir = tempdir().unwrap();
        let content = format!("{}\n{}", valid_event(), pr_event());
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, content).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_receive_all_with_empty_lines() {
        let tmp_dir = tempdir().unwrap();
        let content = format!("{}\n\n{}", valid_event(), pr_event());
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, content).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_receive_all_with_whitespace_lines() {
        let tmp_dir = tempdir().unwrap();
        let content = format!("{}\n  \n{}", valid_event(), pr_event());
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, content).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_receive_all_with_invalid_json_line() {
        let tmp_dir = tempdir().unwrap();
        let content = format!(
            "{}\n{}\n{}",
            valid_event(),
            r#"{"invalid json":"#,
            pr_event()
        );
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, content).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), None);
        // Should succeed but skip the invalid line
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }

    #[test]
    fn test_receive_all_with_event_type_filter() {
        let tmp_dir = tempdir().unwrap();
        let content = format!("{}\n{}", valid_event(), pr_event());
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, content).unwrap();

        let result = receive_all(file_path.to_str().unwrap(), Some("PushEvent".to_string()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_receive_all_with_non_matching_filter() {
        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, valid_event()).unwrap();

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

    #[test]
    fn test_receive_all_with_all_invalid_json_lines() {
        let tmp_dir = tempdir().unwrap();
        let content = format!("{}\n{}", r#"{"bad":"#, r#"{"also":"bad"#);
        let file_path = tmp_dir.path().join("test.json");
        fs::write(&file_path, content).unwrap();

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

    fn valid_event() -> &'static str {
        r#"{"id":"123","type":"PushEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-01T00:00:00Z"}"#
    }

    fn pr_event() -> &'static str {
        r#"{"id":"124","type":"PullRequestEvent","actor":{"id":2,"login":"user2","gravatar_id":"","url":"","avatar_url":"","display_login":"user2"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-02T00:00:00Z"}"#
    }

    fn create_event() -> &'static str {
        r#"{"id":"125","type":"CreateEvent","actor":{"id":1,"login":"user","gravatar_id":"","url":"","avatar_url":"","display_login":"user"},"repo":{"id":1,"name":"repo","url":""},"payload":{},"public":true,"created_at":"2021-01-03T00:00:00Z"}"#
    }

    #[test]
    fn test_save_events_to_output_file() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        let output_file = tmp_dir.path().join("output.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            Some(output_file.to_str().unwrap().to_string()),
            false,
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
        let content = format!("{}\n{}", valid_event(), pr_event());
        fs::write(tmp_dir.path().join("1.json"), content).unwrap();

        let output_file = tmp_dir.path().join("push_only.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            Some("PushEvent".to_string()),
            Some(output_file.to_str().unwrap().to_string()),
            false,
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

        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            Some(output_file.to_str().unwrap().to_string()),
            false,
        );

        assert!(result.is_ok());
        let content = fs::read_to_string(&output_file).unwrap();
        assert!(!content.contains("old content"));
        assert!(content.contains("PushEvent"));
    }

    #[test]
    fn test_save_multiple_events_jsonl_format() {
        let tmp_dir = tempdir().unwrap();
        let content = format!("{}\n{}", valid_event(), create_event());
        fs::write(tmp_dir.path().join("1.json"), content).unwrap();

        let output_file = tmp_dir.path().join("output.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            Some(output_file.to_str().unwrap().to_string()),
            false,
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
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        // Should work fine without output file being specified
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            None,
            false,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn test_save_with_invalid_output_path() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        // Try to save to a non-existent directory
        let invalid_output = "/non/existent/dir/output.jsonl";
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            Some(invalid_output.to_string()),
            false,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_save_empty_filter_result() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        let output_file = tmp_dir.path().join("output.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            Some("PullRequestEvent".to_string()),
            Some(output_file.to_str().unwrap().to_string()),
            false,
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
        for i in 1..=3 {
            fs::write(
                tmp_dir.path().join(format!("file-{}.json", i)),
                valid_event(),
            )
            .unwrap();
        }

        let output_file = tmp_dir.path().join("output.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            Some(output_file.to_str().unwrap().to_string()),
            false,
        );

        assert!(result.is_ok());
        let content = fs::read_to_string(&output_file).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn test_save_with_all_options() {
        let tmp_dir = tempdir().unwrap();
        let content = format!("{}\n{}", valid_event(), pr_event());
        fs::write(tmp_dir.path().join("events.json"), content).unwrap();

        let output_file = tmp_dir.path().join("output.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            true,
            Some("PushEvent".to_string()),
            Some(output_file.to_str().unwrap().to_string()),
            false,
        );

        assert!(result.is_ok());
        assert!(output_file.exists());
    }

    #[test]
    fn test_save_with_quiet_mode() {
        let tmp_dir = tempdir().unwrap();
        fs::write(tmp_dir.path().join("1.json"), valid_event()).unwrap();

        let output_file = tmp_dir.path().join("output.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            Some(output_file.to_str().unwrap().to_string()),
            true,
        );

        assert!(result.is_ok());
        assert!(output_file.exists());
    }

    #[test]
    fn test_save_many_large_files() {
        let tmp_dir = tempdir().unwrap();
        for i in 1..=5 {
            fs::write(
                tmp_dir.path().join(format!("file-{}.json", i)),
                valid_event(),
            )
            .unwrap();
        }

        let output_file = tmp_dir.path().join("output.jsonl");
        let result = check_folder(
            tmp_dir.path().to_str().unwrap(),
            false,
            false,
            None,
            Some(output_file.to_str().unwrap().to_string()),
            false,
        );

        assert!(result.is_ok());
        let content = fs::read_to_string(&output_file).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 5);
    }
}
