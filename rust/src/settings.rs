use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PathEntry {
    pub path: String,
    pub score: f64,
    pub last_accessed: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AppSettings {
    pub profile_name: String,
    pub bucket: String,
    pub prefix: String,
    pub frecent_paths: HashMap<String, Vec<PathEntry>>,
}

fn settings_dir() -> Option<PathBuf> {
    dirs::config_dir().map(|dir| dir.join("s6ui"))
}

fn settings_path() -> Option<PathBuf> {
    settings_dir().map(|dir| dir.join("settings.json"))
}

pub fn load_settings() -> AppSettings {
    let Some(path) = settings_path() else {
        eprintln!("Cannot determine settings path");
        return AppSettings::default();
    };

    load_settings_from_path(&path)
}

fn load_settings_from_path(path: &Path) -> AppSettings {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return AppSettings::default(),
        Err(err) => {
            eprintln!("Failed to read settings file {}: {err}", path.display());
            return AppSettings::default();
        }
    };

    let root: Value = match serde_json::from_str(&contents) {
        Ok(root) => root,
        Err(err) => {
            eprintln!("Failed to parse settings file {}: {err}", path.display());
            return AppSettings::default();
        }
    };

    let mut settings = AppSettings::default();
    settings.profile_name = root
        .get("profile")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    settings.bucket = root
        .get("bucket")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    settings.prefix = root
        .get("prefix")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    if let Some(profiles) = root.get("frecent_paths").and_then(Value::as_object) {
        for (profile, entries) in profiles {
            let Some(entries) = entries.as_array() else {
                continue;
            };

            let mut profile_entries = Vec::new();
            for entry in entries {
                let Some(path) = entry.get("path").and_then(Value::as_str) else {
                    continue;
                };
                if path.is_empty() {
                    continue;
                }

                profile_entries.push(PathEntry {
                    path: path.to_string(),
                    score: entry.get("score").and_then(Value::as_f64).unwrap_or(0.0),
                    last_accessed: entry
                        .get("last_accessed")
                        .and_then(Value::as_i64)
                        .unwrap_or(0),
                });
            }

            if !profile_entries.is_empty() {
                settings
                    .frecent_paths
                    .insert(profile.clone(), profile_entries);
            }
        }
    }

    settings
}

pub fn save_settings(settings: &AppSettings) -> io::Result<()> {
    let path = settings_path().ok_or_else(|| io::Error::other("Cannot determine settings path"))?;
    save_settings_to_path(settings, &path)
}

fn save_settings_to_path(settings: &AppSettings, path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut frecent_paths = Map::new();
    for (profile, entries) in &settings.frecent_paths {
        let entries = entries
            .iter()
            .map(|entry| {
                Value::Object(Map::from_iter([
                    ("path".to_string(), Value::String(entry.path.clone())),
                    (
                        "score".to_string(),
                        Value::Number(
                            serde_json::Number::from_f64(entry.score)
                                .unwrap_or_else(|| serde_json::Number::from(0)),
                        ),
                    ),
                    (
                        "last_accessed".to_string(),
                        Value::Number(entry.last_accessed.into()),
                    ),
                ]))
            })
            .collect();
        frecent_paths.insert(profile.clone(), Value::Array(entries));
    }

    let root = Value::Object(Map::from_iter([
        (
            "profile".to_string(),
            Value::String(settings.profile_name.clone()),
        ),
        ("bucket".to_string(), Value::String(settings.bucket.clone())),
        ("prefix".to_string(), Value::String(settings.prefix.clone())),
        ("frecent_paths".to_string(), Value::Object(frecent_paths)),
    ]));

    let mut file = File::create(path)?;
    serde_json::to_writer_pretty(&mut file, &root).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_settings_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir()
            .join(format!(
                "s6ui-settings-tests-{}-{nanos}",
                std::process::id()
            ))
            .join(name)
    }

    #[test]
    fn settings_round_trip_preserves_shape() {
        let path = unique_settings_path("settings.json");
        let mut settings = AppSettings {
            profile_name: "default".to_string(),
            bucket: "bucket".to_string(),
            prefix: "prefix/".to_string(),
            frecent_paths: HashMap::new(),
        };
        settings.frecent_paths.insert(
            "default".to_string(),
            vec![
                PathEntry {
                    path: "s3://bucket/prefix/".to_string(),
                    score: 4.5,
                    last_accessed: 123,
                },
                PathEntry {
                    path: "s3://bucket/other/".to_string(),
                    score: 1.0,
                    last_accessed: 456,
                },
            ],
        );

        save_settings_to_path(&settings, &path).unwrap();
        let loaded = load_settings_from_path(&path);
        assert_eq!(loaded, settings);

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn load_settings_ignores_invalid_recent_entries() {
        let path = unique_settings_path("settings.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            r#"{
  "profile": "default",
  "bucket": "bucket",
  "prefix": "prefix/",
  "frecent_paths": {
    "default": [
      { "path": "s3://bucket/prefix/", "score": 2.0, "last_accessed": 10 },
      { "path": "", "score": 9.0, "last_accessed": 11 },
      { "score": 5.0, "last_accessed": 12 },
      "bad"
    ]
  }
}"#,
        )
        .unwrap();

        let loaded = load_settings_from_path(&path);
        assert_eq!(loaded.profile_name, "default");
        assert_eq!(loaded.bucket, "bucket");
        assert_eq!(loaded.prefix, "prefix/");
        assert_eq!(
            loaded.frecent_paths.get("default"),
            Some(&vec![PathEntry {
                path: "s3://bucket/prefix/".to_string(),
                score: 2.0,
                last_accessed: 10,
            }])
        );

        let _ = fs::remove_dir_all(path.parent().unwrap());
    }
}
