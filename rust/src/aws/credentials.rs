use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// An AWS profile with credentials and configuration
#[derive(Debug, Clone, Default)]
pub struct AwsProfile {
    pub name: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub region: String,
    pub endpoint_url: String,
}

/// Parse an INI file into sections of key-value pairs.
/// Handles `[section]` and `[profile section]` (config file format).
fn parse_ini_file(path: &PathBuf) -> HashMap<String, HashMap<String, String>> {
    let mut sections: HashMap<String, HashMap<String, String>> = HashMap::new();
    let content = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return sections,
    };

    let mut current_section = String::new();

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }

        if line.starts_with('[') && line.ends_with(']') {
            let mut section = line[1..line.len() - 1].to_string();
            // Remove "profile " prefix if present (from config file)
            if let Some(stripped) = section.strip_prefix("profile ") {
                section = stripped.to_string();
            }
            current_section = section.trim().to_string();
        } else if let Some(eq_pos) = line.find('=') {
            if !current_section.is_empty() {
                let key = line[..eq_pos].trim().to_string();
                let value = line[eq_pos + 1..].trim().to_string();
                sections
                    .entry(current_section.clone())
                    .or_default()
                    .insert(key, value);
            }
        }
    }

    sections
}

/// Load all AWS profiles from ~/.aws/credentials and ~/.aws/config.
/// Merges credentials and config by profile name.
pub fn load_aws_profiles() -> Vec<AwsProfile> {
    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return Vec::new(),
    };

    let creds = parse_ini_file(&home.join(".aws").join("credentials"));
    let config = parse_ini_file(&home.join(".aws").join("config"));

    let mut profiles = Vec::new();

    // Build profile list from credentials file
    for (name, values) in &creds {
        let access_key_id = values.get("aws_access_key_id").cloned().unwrap_or_default();
        let secret_access_key = values
            .get("aws_secret_access_key")
            .cloned()
            .unwrap_or_default();

        if access_key_id.is_empty() || secret_access_key.is_empty() {
            continue;
        }

        let session_token = values.get("aws_session_token").cloned().unwrap_or_default();

        // Look for region and endpoint_url in config
        let mut region = String::new();
        let mut endpoint_url = String::new();
        if let Some(cfg) = config.get(name) {
            if let Some(r) = cfg.get("region") {
                region = r.clone();
            }
            if let Some(e) = cfg.get("endpoint_url") {
                endpoint_url = e.clone();
            }
        }

        if region.is_empty() {
            region = "us-east-1".to_string();
        }

        profiles.push(AwsProfile {
            name: name.clone(),
            access_key_id,
            secret_access_key,
            session_token,
            region,
            endpoint_url,
        });
    }

    // Also check config-only profiles (SSO profiles won't have credentials,
    // but some profiles might have static creds in config)
    for (name, values) in &config {
        // Skip sso-session sections
        if name.starts_with("sso-session ") {
            continue;
        }

        // Skip if already processed from credentials
        if profiles.iter().any(|p| &p.name == name) {
            continue;
        }

        // Check for static credentials in config
        let access_key_id = values.get("aws_access_key_id").cloned().unwrap_or_default();
        let secret_access_key = values
            .get("aws_secret_access_key")
            .cloned()
            .unwrap_or_default();

        if access_key_id.is_empty() || secret_access_key.is_empty() {
            continue;
        }

        let session_token = values.get("aws_session_token").cloned().unwrap_or_default();
        let region = values
            .get("region")
            .cloned()
            .unwrap_or_else(|| "us-east-1".to_string());
        let endpoint_url = values.get("endpoint_url").cloned().unwrap_or_default();

        profiles.push(AwsProfile {
            name: name.clone(),
            access_key_id,
            secret_access_key,
            session_token,
            region,
            endpoint_url,
        });
    }

    profiles
}

/// Find the default profile index based on AWS_PROFILE env var or "default" name.
pub fn default_profile_index(profiles: &[AwsProfile]) -> usize {
    if let Ok(env_profile) = std::env::var("AWS_PROFILE") {
        if let Some(idx) = profiles.iter().position(|p| p.name == env_profile) {
            return idx;
        }
    }
    // Fall back to "default" profile
    profiles
        .iter()
        .position(|p| p.name == "default")
        .unwrap_or(0)
}
