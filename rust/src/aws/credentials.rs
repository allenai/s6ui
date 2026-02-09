use ring::digest;
use std::collections::HashMap;
use std::fs;
use std::io::BufRead;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default)]
pub struct AWSProfile {
    pub name: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub endpoint_url: String,
    pub session_token: String,
    pub expiration: i64,

    // SSO fields
    pub sso_start_url: String,
    pub sso_region: String,
    pub sso_account_id: String,
    pub sso_role_name: String,
    pub sso_session_name: String,
}

type IniSections = HashMap<String, HashMap<String, String>>;

fn parse_ini_file(path: &Path) -> IniSections {
    let mut sections = IniSections::new();
    let file = match fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return sections,
    };

    let reader = std::io::BufReader::new(file);
    let mut current_section = String::new();

    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => continue,
        };
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }

        if line.starts_with('[') && line.ends_with(']') {
            let mut section = &line[1..line.len() - 1];
            // Remove "profile " prefix if present (from config file)
            if let Some(stripped) = section.strip_prefix("profile ") {
                section = stripped;
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

fn sha1_hex(input: &str) -> String {
    let digest = digest::digest(&digest::SHA1_FOR_LEGACY_USE_ONLY, input.as_bytes());
    digest
        .as_ref()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

fn parse_iso8601(timestamp: &str) -> i64 {
    // Parse format: "2024-01-15T12:34:56UTC" or "2024-01-15T12:34:56Z"
    // Simple parser - just extract the components
    if timestamp.len() < 19 {
        return 0;
    }
    let parts: Vec<&str> = timestamp.split('T').collect();
    if parts.len() != 2 {
        return 0;
    }
    let date_parts: Vec<&str> = parts[0].split('-').collect();
    if date_parts.len() != 3 {
        return 0;
    }
    let time_str = parts[1].trim_end_matches(|c: char| !c.is_ascii_digit() && c != ':');
    let time_parts: Vec<&str> = time_str.split(':').collect();
    if time_parts.len() != 3 {
        return 0;
    }

    let year: i64 = date_parts[0].parse().unwrap_or(0);
    let month: i64 = date_parts[1].parse().unwrap_or(0);
    let day: i64 = date_parts[2].parse().unwrap_or(0);
    let hour: i64 = time_parts[0].parse().unwrap_or(0);
    let min: i64 = time_parts[1].parse().unwrap_or(0);
    let sec: i64 = time_parts[2].parse().unwrap_or(0);

    // Approximate UTC timestamp (not exact but sufficient for expiry checks)
    // Days from epoch to year
    let mut days: i64 = 0;
    for y in 1970..year {
        days += if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
            366
        } else {
            365
        };
    }
    let month_days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    for m in 1..month {
        days += month_days[m as usize];
        if m == 2 && is_leap {
            days += 1;
        }
    }
    days += day - 1;

    days * 86400 + hour * 3600 + min * 60 + sec
}

fn get_sso_cached_token(sso_start_url: &str, sso_session_name: &str) -> Option<String> {
    let home = dirs::home_dir()?;

    let hash_input = if sso_session_name.is_empty() {
        sso_start_url
    } else {
        sso_session_name
    };
    let hash = sha1_hex(hash_input);
    let cache_path = home.join(".aws/sso/cache").join(format!("{}.json", hash));

    let data = fs::read_to_string(&cache_path).ok()?;
    let json: serde_json::Value = serde_json::from_str(&data).ok()?;

    // Check expiration
    if let Some(expires_at) = json.get("expiresAt").and_then(|v| v.as_str()) {
        let expiration = parse_iso8601(expires_at);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        if expiration > 0 && now >= expiration {
            eprintln!("aws: SSO token expired at {}", expires_at);
            return None;
        }
    }

    json.get("accessToken")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn get_sso_credentials(profile: &mut AWSProfile) -> bool {
    let access_token = match get_sso_cached_token(&profile.sso_start_url, &profile.sso_session_name)
    {
        Some(t) => t,
        None => {
            eprintln!(
                "aws: SSO credentials expired for profile '{}'. Run: aws sso login --profile {}",
                profile.name, profile.name
            );
            return false;
        }
    };

    let url = format!(
        "https://portal.sso.{}.amazonaws.com/federation/credentials?account_id={}&role_name={}",
        profile.sso_region, profile.sso_account_id, profile.sso_role_name
    );

    // Use a blocking reqwest client for SSO credential fetching
    let client = match reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("aws: failed to create HTTP client for SSO: {}", e);
            return false;
        }
    };

    let response = match client
        .get(&url)
        .header("x-amz-sso_bearer_token", &access_token)
        .send()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("aws: SSO API request failed for '{}': {}", profile.name, e);
            return false;
        }
    };

    if !response.status().is_success() {
        eprintln!(
            "aws: SSO API returned HTTP {} for '{}'",
            response.status(),
            profile.name
        );
        return false;
    }

    let body = match response.text() {
        Ok(b) => b,
        Err(e) => {
            eprintln!("aws: failed to read SSO response: {}", e);
            return false;
        }
    };

    let json: serde_json::Value = match serde_json::from_str(&body) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("aws: failed to parse SSO response: {}", e);
            return false;
        }
    };

    let creds = match json.get("roleCredentials") {
        Some(c) => c,
        None => {
            eprintln!("aws: SSO response missing roleCredentials");
            return false;
        }
    };

    if let Some(val) = creds.get("accessKeyId").and_then(|v| v.as_str()) {
        profile.access_key_id = val.to_string();
    }
    if let Some(val) = creds.get("secretAccessKey").and_then(|v| v.as_str()) {
        profile.secret_access_key = val.to_string();
    }
    if let Some(val) = creds.get("sessionToken").and_then(|v| v.as_str()) {
        profile.session_token = val.to_string();
    }
    if let Some(exp) = creds.get("expiration").and_then(|v| v.as_i64()) {
        profile.expiration = exp / 1000;
    }

    eprintln!(
        "aws: successfully retrieved SSO credentials for '{}'",
        profile.name
    );
    true
}

fn resolve_sso_session(profile: &mut AWSProfile, config: &IniSections) {
    if !profile.sso_start_url.is_empty() {
        return;
    }

    let cfg = match config.get(&profile.name) {
        Some(c) => c,
        None => return,
    };

    let session_ref = match cfg.get("sso_session") {
        Some(s) => s.clone(),
        None => return,
    };

    let session_key = format!("sso-session {}", session_ref);
    let session = match config.get(&session_key) {
        Some(s) => s,
        None => {
            eprintln!(
                "aws: profile '{}' references sso_session '{}' but it doesn't exist",
                profile.name, session_ref
            );
            return;
        }
    };

    profile.sso_session_name = session_ref;

    if let Some(url) = session.get("sso_start_url") {
        profile.sso_start_url = url.clone();
    }
    if let Some(region) = session.get("sso_region") {
        profile.sso_region = region.clone();
    }
}

/// Load all AWS profiles from ~/.aws/credentials and ~/.aws/config
pub fn load_aws_profiles() -> Vec<AWSProfile> {
    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return vec![],
    };

    let creds = parse_ini_file(&home.join(".aws/credentials"));
    let config = parse_ini_file(&home.join(".aws/config"));

    let mut profiles = Vec::new();

    // Build from credentials file
    for (name, values) in &creds {
        let mut profile = AWSProfile {
            name: name.clone(),
            ..Default::default()
        };

        if let Some(v) = values.get("aws_access_key_id") {
            profile.access_key_id = v.clone();
        }
        if let Some(v) = values.get("aws_secret_access_key") {
            profile.secret_access_key = v.clone();
        }
        if let Some(v) = values.get("aws_session_token") {
            profile.session_token = v.clone();
        }

        // Merge config file values
        if let Some(cfg) = config.get(name) {
            if let Some(v) = cfg.get("region") {
                profile.region = v.clone();
            }
            if let Some(v) = cfg.get("endpoint_url") {
                profile.endpoint_url = v.clone();
            }
            if let Some(v) = cfg.get("sso_start_url") {
                profile.sso_start_url = v.clone();
            }
            if let Some(v) = cfg.get("sso_region") {
                profile.sso_region = v.clone();
            }
            if let Some(v) = cfg.get("sso_account_id") {
                profile.sso_account_id = v.clone();
            }
            if let Some(v) = cfg.get("sso_role_name") {
                profile.sso_role_name = v.clone();
            }

            resolve_sso_session(&mut profile, &config);

            // Re-parse account/role from profile (may override session)
            if let Some(v) = cfg.get("sso_account_id") {
                profile.sso_account_id = v.clone();
            }
            if let Some(v) = cfg.get("sso_role_name") {
                profile.sso_role_name = v.clone();
            }
        }

        if profile.region.is_empty() {
            profile.region = "us-east-1".to_string();
        }

        let has_keys = !profile.access_key_id.is_empty() && !profile.secret_access_key.is_empty();
        let has_sso = !profile.sso_start_url.is_empty()
            && !profile.sso_account_id.is_empty()
            && !profile.sso_role_name.is_empty();

        if has_keys || has_sso {
            profiles.push(profile);
        }
    }

    // Check for SSO-only profiles in config (not in credentials)
    for (name, values) in &config {
        if name.starts_with("sso-session ") {
            continue;
        }
        if profiles.iter().any(|p| p.name == *name) {
            continue;
        }

        let mut profile = AWSProfile {
            name: name.clone(),
            ..Default::default()
        };

        if let Some(v) = values.get("sso_start_url") {
            profile.sso_start_url = v.clone();
        }
        if let Some(v) = values.get("sso_region") {
            profile.sso_region = v.clone();
        }

        resolve_sso_session(&mut profile, &config);

        if let Some(v) = values.get("sso_account_id") {
            profile.sso_account_id = v.clone();
        }
        if let Some(v) = values.get("sso_role_name") {
            profile.sso_role_name = v.clone();
        }
        if let Some(v) = values.get("region") {
            profile.region = v.clone();
        }
        if profile.region.is_empty() {
            profile.region = "us-east-1".to_string();
        }

        let has_sso = !profile.sso_start_url.is_empty()
            && !profile.sso_account_id.is_empty()
            && !profile.sso_role_name.is_empty();

        if has_sso {
            profiles.push(profile);
        }
    }

    // Resolve SSO credentials
    for profile in &mut profiles {
        if !profile.sso_start_url.is_empty() && profile.access_key_id.is_empty() {
            if !get_sso_credentials(profile) {
                eprintln!(
                    "aws: profile '{}' uses SSO but credentials could not be loaded",
                    profile.name
                );
            }
        }
    }

    // Remove profiles without valid credentials
    profiles.retain(|p| {
        let has = !p.access_key_id.is_empty() && !p.secret_access_key.is_empty();
        if !has {
            eprintln!("aws: removing profile '{}' - no valid credentials", p.name);
        }
        has
    });

    profiles
}

/// Refresh credentials for a specific profile.
/// Returns true if credentials were successfully refreshed.
pub fn refresh_profile_credentials(profile: &mut AWSProfile) -> bool {
    eprintln!("aws: refreshing credentials for '{}'", profile.name);

    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return false,
    };

    let creds = parse_ini_file(&home.join(".aws/credentials"));
    let config = parse_ini_file(&home.join(".aws/config"));

    if let Some(cred_values) = creds.get(&profile.name) {
        profile.access_key_id = cred_values
            .get("aws_access_key_id")
            .cloned()
            .unwrap_or_default();
        profile.secret_access_key = cred_values
            .get("aws_secret_access_key")
            .cloned()
            .unwrap_or_default();
        profile.session_token = cred_values
            .get("aws_session_token")
            .cloned()
            .unwrap_or_default();
    }

    if let Some(cfg) = config.get(&profile.name) {
        if let Some(v) = cfg.get("region") {
            profile.region = v.clone();
        }
        profile.endpoint_url = cfg.get("endpoint_url").cloned().unwrap_or_default();
        profile.sso_start_url = cfg.get("sso_start_url").cloned().unwrap_or_default();
        profile.sso_region = cfg.get("sso_region").cloned().unwrap_or_default();
        profile.sso_account_id = cfg.get("sso_account_id").cloned().unwrap_or_default();
        profile.sso_role_name = cfg.get("sso_role_name").cloned().unwrap_or_default();

        profile.sso_session_name.clear();
        resolve_sso_session(profile, &config);

        if let Some(v) = cfg.get("sso_account_id") {
            profile.sso_account_id = v.clone();
        }
        if let Some(v) = cfg.get("sso_role_name") {
            profile.sso_role_name = v.clone();
        }
    }

    if profile.region.is_empty() {
        profile.region = "us-east-1".to_string();
    }

    if !profile.sso_start_url.is_empty() && profile.access_key_id.is_empty() {
        if !get_sso_credentials(profile) {
            eprintln!(
                "aws: failed to refresh SSO credentials for '{}'",
                profile.name
            );
            return false;
        }
    }

    if profile.access_key_id.is_empty() || profile.secret_access_key.is_empty() {
        eprintln!(
            "aws: profile '{}' has no valid credentials after refresh",
            profile.name
        );
        return false;
    }

    true
}
