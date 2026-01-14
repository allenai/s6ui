#include "aws_signer.h"
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <ctime>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <vector>

static std::string to_hex(const unsigned char* data, size_t len) {
    std::ostringstream ss;
    ss << std::hex << std::setfill('0');
    for (size_t i = 0; i < len; ++i) {
        ss << std::setw(2) << static_cast<int>(data[i]);
    }
    return ss.str();
}

static std::string sha256(const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(data.c_str()), data.size(), hash);
    return to_hex(hash, SHA256_DIGEST_LENGTH);
}

static std::string hmac_sha256_raw(const std::string& key, const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    unsigned int len = SHA256_DIGEST_LENGTH;
    HMAC(EVP_sha256(), key.c_str(), key.size(),
         reinterpret_cast<const unsigned char*>(data.c_str()), data.size(),
         hash, &len);
    return std::string(reinterpret_cast<char*>(hash), SHA256_DIGEST_LENGTH);
}

static std::string hmac_sha256_hex(const std::string& key, const std::string& data) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    unsigned int len = SHA256_DIGEST_LENGTH;
    HMAC(EVP_sha256(), key.c_str(), key.size(),
         reinterpret_cast<const unsigned char*>(data.c_str()), data.size(),
         hash, &len);
    return to_hex(hash, SHA256_DIGEST_LENGTH);
}

static std::string get_timestamp() {
    time_t now = time(nullptr);
    struct tm tm_buf;
    gmtime_r(&now, &tm_buf);
    char buf[32];
    strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm_buf);
    return buf;
}

static std::string get_date(const std::string& timestamp) {
    return timestamp.substr(0, 8);
}

// URL encode a path segment (everything except alphanumerics and -_.~)
// This is used for encoding path components for AWS signature
static std::string uri_encode(const std::string& value, bool encode_slash = true) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (unsigned char c : value) {
        // Keep alphanumeric and other accepted characters intact
        if (isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else if (c == '/' && !encode_slash) {
            escaped << c;  // Keep slashes as-is when encoding paths
        } else {
            // Any other characters are percent-encoded
            escaped << '%' << std::uppercase << std::setw(2)
                    << static_cast<int>(c);
        }
    }

    return escaped.str();
}

static std::string sort_query_string(const std::string& query) {
    if (query.empty()) return "";

    // Parse query string into key-value pairs
    std::vector<std::pair<std::string, std::string>> params;
    std::istringstream iss(query);
    std::string param;
    while (std::getline(iss, param, '&')) {
        size_t eq = param.find('=');
        if (eq != std::string::npos) {
            params.emplace_back(param.substr(0, eq), param.substr(eq + 1));
        } else {
            params.emplace_back(param, "");
        }
    }

    // Sort by key (and value if keys are equal)
    std::sort(params.begin(), params.end());

    // Rebuild query string
    std::ostringstream result;
    for (size_t i = 0; i < params.size(); ++i) {
        if (i > 0) result << "&";
        result << params[i].first;
        if (!params[i].second.empty() || params[i].first.find('=') != std::string::npos) {
            result << "=" << params[i].second;
        }
    }
    return result.str();
}

AWSSignedRequest aws_sign_request(
    const std::string& method,
    const std::string& host,
    const std::string& path,
    const std::string& query,
    const std::string& region,
    const std::string& service,
    const std::string& access_key,
    const std::string& secret_key,
    const std::string& payload,
    const std::string& session_token
) {
    std::string timestamp = get_timestamp();
    std::string date = get_date(timestamp);

    // Payload hash
    std::string payload_hash = sha256(payload);

    // Canonical URI (path) - must be URI-encoded, but keep slashes
    std::string canonical_uri = path.empty() ? "/" : uri_encode(path, false);

    // Canonical query string (must be sorted alphabetically by parameter name)
    std::string canonical_query = sort_query_string(query);

    // Canonical headers (must be sorted, lowercase)
    std::ostringstream canonical_headers_ss;
    canonical_headers_ss << "host:" << host << "\n";
    canonical_headers_ss << "x-amz-content-sha256:" << payload_hash << "\n";
    canonical_headers_ss << "x-amz-date:" << timestamp << "\n";
    if (!session_token.empty()) {
        canonical_headers_ss << "x-amz-security-token:" << session_token << "\n";
    }
    std::string canonical_headers = canonical_headers_ss.str();

    std::string signed_headers = "host;x-amz-content-sha256;x-amz-date";
    if (!session_token.empty()) {
        signed_headers += ";x-amz-security-token";
    }

    // Canonical request
    std::ostringstream canonical_request_ss;
    canonical_request_ss << method << "\n";
    canonical_request_ss << canonical_uri << "\n";
    canonical_request_ss << canonical_query << "\n";
    canonical_request_ss << canonical_headers << "\n";
    canonical_request_ss << signed_headers << "\n";
    canonical_request_ss << payload_hash;
    std::string canonical_request = canonical_request_ss.str();

    // String to sign
    std::string algorithm = "AWS4-HMAC-SHA256";
    std::string credential_scope = date + "/" + region + "/" + service + "/aws4_request";
    std::ostringstream string_to_sign_ss;
    string_to_sign_ss << algorithm << "\n";
    string_to_sign_ss << timestamp << "\n";
    string_to_sign_ss << credential_scope << "\n";
    string_to_sign_ss << sha256(canonical_request);
    std::string string_to_sign = string_to_sign_ss.str();

    // Signing key
    std::string k_secret = "AWS4" + secret_key;
    std::string k_date = hmac_sha256_raw(k_secret, date);
    std::string k_region = hmac_sha256_raw(k_date, region);
    std::string k_service = hmac_sha256_raw(k_region, service);
    std::string k_signing = hmac_sha256_raw(k_service, "aws4_request");

    // Signature
    std::string signature = hmac_sha256_hex(k_signing, string_to_sign);

    // Authorization header
    std::ostringstream auth_ss;
    auth_ss << algorithm << " ";
    auth_ss << "Credential=" << access_key << "/" << credential_scope << ", ";
    auth_ss << "SignedHeaders=" << signed_headers << ", ";
    auth_ss << "Signature=" << signature;
    std::string authorization = auth_ss.str();

    // Build result
    AWSSignedRequest result;
    std::ostringstream url_ss;
    url_ss << "https://" << host << canonical_uri;
    if (!canonical_query.empty()) {
        url_ss << "?" << canonical_query;
    }
    result.url = url_ss.str();

    result.headers["Host"] = host;
    result.headers["x-amz-date"] = timestamp;
    result.headers["x-amz-content-sha256"] = payload_hash;
    result.headers["Authorization"] = authorization;
    if (!session_token.empty()) {
        result.headers["x-amz-security-token"] = session_token;
    }

    return result;
}

// URL encode a string (RFC 3986)
static std::string url_encode(const std::string& value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;

    for (char c : value) {
        // Keep alphanumeric and other accepted characters intact
        if (isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            // Any other characters are percent-encoded
            escaped << '%' << std::uppercase << std::setw(2)
                    << static_cast<int>(static_cast<unsigned char>(c));
        }
    }

    return escaped.str();
}

std::string aws_generate_presigned_url(
    const std::string& bucket,
    const std::string& key,
    const std::string& region,
    const std::string& access_key,
    const std::string& secret_key,
    const std::string& session_token,
    int expires_seconds
) {
    std::string timestamp = get_timestamp();
    std::string date = get_date(timestamp);

    // Build the host
    std::string host = bucket + ".s3." + region + ".amazonaws.com";

    // Build the canonical URI (path) - must be URL encoded, but keep slashes
    std::string canonical_uri = "/" + uri_encode(key, false);

    // For presigned URLs, we use UNSIGNED-PAYLOAD
    const std::string payload_hash = "UNSIGNED-PAYLOAD";

    // Build credential scope
    std::string credential_scope = date + "/" + region + "/s3/aws4_request";
    std::string credential = access_key + "/" + credential_scope;

    // Build canonical query string (must be sorted alphabetically)
    // Note: We build this WITHOUT the signature first, then add signature at the end
    std::ostringstream query_ss;
    query_ss << "X-Amz-Algorithm=AWS4-HMAC-SHA256";
    query_ss << "&X-Amz-Credential=" << url_encode(credential);
    query_ss << "&X-Amz-Date=" << timestamp;
    query_ss << "&X-Amz-Expires=" << expires_seconds;
    if (!session_token.empty()) {
        query_ss << "&X-Amz-Security-Token=" << url_encode(session_token);
    }
    query_ss << "&X-Amz-SignedHeaders=host";
    std::string canonical_query = query_ss.str();

    // Canonical headers (just host for presigned URLs)
    std::string canonical_headers = "host:" + host + "\n";
    std::string signed_headers = "host";

    // Canonical request
    std::ostringstream canonical_request_ss;
    canonical_request_ss << "GET\n";
    canonical_request_ss << canonical_uri << "\n";
    canonical_request_ss << canonical_query << "\n";
    canonical_request_ss << canonical_headers << "\n";
    canonical_request_ss << signed_headers << "\n";
    canonical_request_ss << payload_hash;
    std::string canonical_request = canonical_request_ss.str();

    // String to sign
    std::string algorithm = "AWS4-HMAC-SHA256";
    std::ostringstream string_to_sign_ss;
    string_to_sign_ss << algorithm << "\n";
    string_to_sign_ss << timestamp << "\n";
    string_to_sign_ss << credential_scope << "\n";
    string_to_sign_ss << sha256(canonical_request);
    std::string string_to_sign = string_to_sign_ss.str();

    // Signing key
    std::string k_secret = "AWS4" + secret_key;
    std::string k_date = hmac_sha256_raw(k_secret, date);
    std::string k_region = hmac_sha256_raw(k_date, region);
    std::string k_service = hmac_sha256_raw(k_region, "s3");
    std::string k_signing = hmac_sha256_raw(k_service, "aws4_request");

    // Signature
    std::string signature = hmac_sha256_hex(k_signing, string_to_sign);

    // Build final URL
    std::ostringstream url_ss;
    url_ss << "https://" << host << canonical_uri;
    url_ss << "?" << canonical_query;
    url_ss << "&X-Amz-Signature=" << signature;

    return url_ss.str();
}
