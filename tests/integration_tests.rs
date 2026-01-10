use etl_rust::{Config, run};
use std::fs;
use tempfile::tempdir;

#[test]
fn test_run_with_invalid_path() {
    let config = Config {
        path_to_data: String::from("/non/existent/path"),
        dry_run: false,
    };

    let result = run(config);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unable to read folder"));
}

#[test]
fn test_run_with_empty_folder() {
    let tmp_dir = tempdir().unwrap();

    let config = Config {
        path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
        dry_run: false,
    };

    let result = run(config);
    assert!(result.is_ok());
}

#[test]
fn test_run_with_single_json_file() {
    let tmp_dir = tempdir().unwrap();
    let file_path = tmp_dir.path().join("1.json");
    fs::write(&file_path, r#"{"type":"PushEvent"}"#).unwrap();

    let config = Config {
        path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
        dry_run: false,
    };

    let result = run(config);
    assert!(result.is_ok());
}

#[test]
fn test_run_with_multiple_json_files() {
    let tmp_dir = tempdir().unwrap();

    for i in 1..=4 {
        let file_path = tmp_dir.path().join(format!("file-{}.json", i));
        fs::write(&file_path, r#"{"type":"PushEvent"}"#).unwrap();
    }

    let config = Config {
        path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
        dry_run: false,
    };

    let result = run(config);
    assert!(result.is_ok());
}

#[test]
fn test_run_with_invalid_json() {
    let tmp_dir = tempdir().unwrap();
    let file_path = tmp_dir.path().join("bad.json");
    fs::write(&file_path, r#"{"type": }"#).unwrap();

    let config = Config {
        path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
        dry_run: false,
    };

    let result = run(config);
    assert!(result.is_ok()); // prints warning
}

#[test]
fn test_run_dry_run_like() {
    let tmp_dir = tempdir().unwrap();

    for i in 1..=2 {
        let file_path = tmp_dir.path().join(format!("dry-{}.json", i));
        fs::write(&file_path, r#"{"type":"PushEvent"}"#).unwrap();
    }

    let config = Config {
        path_to_data: tmp_dir.path().to_str().unwrap().to_string(),
        dry_run: false,
    };

    let result = run(config);
    assert!(result.is_ok());
}
