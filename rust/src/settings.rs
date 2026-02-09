use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathEntry {
    pub path: String,
    #[serde(default)]
    pub score: f64,
    #[serde(default)]
    pub last_accessed: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppSettings {
    #[serde(default)]
    pub profile: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub frecent_paths: HashMap<String, Vec<PathEntry>>,
}

fn settings_dir() -> Option<PathBuf> {
    // On macOS: ~/Library/Application Support/s6ui
    // On Linux: ~/.config/s6ui (via dirs crate)
    dirs::config_dir().map(|d| d.join("s6ui"))
}

fn settings_path() -> Option<PathBuf> {
    settings_dir().map(|d| d.join("settings.json"))
}

/// Load settings from the platform config directory.
/// Returns default settings if file is missing or invalid.
pub fn load_settings() -> AppSettings {
    let path = match settings_path() {
        Some(p) => p,
        None => {
            eprintln!("settings: cannot determine config path");
            return AppSettings::default();
        }
    };

    let data = match fs::read_to_string(&path) {
        Ok(d) => d,
        Err(_) => {
            eprintln!("settings: no settings file at {}", path.display());
            return AppSettings::default();
        }
    };

    match serde_json::from_str(&data) {
        Ok(s) => {
            eprintln!(
                "settings: loaded from {}",
                path.display()
            );
            s
        }
        Err(e) => {
            eprintln!("settings: failed to parse {}: {}", path.display(), e);
            AppSettings::default()
        }
    }
}

/// Save settings to the platform config directory.
/// Creates directory if needed.
pub fn save_settings(settings: &AppSettings) {
    let dir = match settings_dir() {
        Some(d) => d,
        None => {
            eprintln!("settings: cannot determine config directory");
            return;
        }
    };

    if let Err(e) = fs::create_dir_all(&dir) {
        eprintln!("settings: failed to create directory {}: {}", dir.display(), e);
        return;
    }

    let path = dir.join("settings.json");
    match serde_json::to_string_pretty(settings) {
        Ok(json) => {
            if let Err(e) = fs::write(&path, json) {
                eprintln!("settings: failed to write {}: {}", path.display(), e);
            } else {
                eprintln!("settings: saved to {}", path.display());
            }
        }
        Err(e) => {
            eprintln!("settings: failed to serialize: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_settings_roundtrip() {
        let mut settings = AppSettings::default();
        settings.profile = "test-profile".to_string();
        settings.bucket = "my-bucket".to_string();
        settings.prefix = "some/prefix/".to_string();
        settings.frecent_paths.insert(
            "test-profile".to_string(),
            vec![
                PathEntry {
                    path: "s3://my-bucket/foo/".to_string(),
                    score: 5.0,
                    last_accessed: 1700000000,
                },
                PathEntry {
                    path: "s3://my-bucket/bar/".to_string(),
                    score: 2.0,
                    last_accessed: 1699999000,
                },
            ],
        );

        let json = serde_json::to_string_pretty(&settings).unwrap();
        let loaded: AppSettings = serde_json::from_str(&json).unwrap();

        assert_eq!(loaded.profile, "test-profile");
        assert_eq!(loaded.bucket, "my-bucket");
        assert_eq!(loaded.prefix, "some/prefix/");
        assert_eq!(loaded.frecent_paths.len(), 1);
        let entries = &loaded.frecent_paths["test-profile"];
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "s3://my-bucket/foo/");
        assert!((entries[0].score - 5.0).abs() < f64::EPSILON);
    }
}
