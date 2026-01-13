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

    // Canonical URI (path)
    std::string canonical_uri = path.empty() ? "/" : path;

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
