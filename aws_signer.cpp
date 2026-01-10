#include "aws_signer.h"
#include <CommonCrypto/CommonHMAC.h>
#include <CommonCrypto/CommonDigest.h>
#include <ctime>
#include <sstream>
#include <iomanip>
#include <algorithm>

static std::string to_hex(const unsigned char* data, size_t len) {
    std::ostringstream ss;
    ss << std::hex << std::setfill('0');
    for (size_t i = 0; i < len; ++i) {
        ss << std::setw(2) << static_cast<int>(data[i]);
    }
    return ss.str();
}

static std::string sha256(const std::string& data) {
    unsigned char hash[CC_SHA256_DIGEST_LENGTH];
    CC_SHA256(data.c_str(), static_cast<CC_LONG>(data.size()), hash);
    return to_hex(hash, CC_SHA256_DIGEST_LENGTH);
}

static std::string hmac_sha256_raw(const std::string& key, const std::string& data) {
    unsigned char hash[CC_SHA256_DIGEST_LENGTH];
    CCHmac(kCCHmacAlgSHA256, key.c_str(), key.size(), data.c_str(), data.size(), hash);
    return std::string(reinterpret_cast<char*>(hash), CC_SHA256_DIGEST_LENGTH);
}

static std::string hmac_sha256_hex(const std::string& key, const std::string& data) {
    unsigned char hash[CC_SHA256_DIGEST_LENGTH];
    CCHmac(kCCHmacAlgSHA256, key.c_str(), key.size(), data.c_str(), data.size(), hash);
    return to_hex(hash, CC_SHA256_DIGEST_LENGTH);
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

AWSSignedRequest aws_sign_request(
    const std::string& method,
    const std::string& host,
    const std::string& path,
    const std::string& query,
    const std::string& region,
    const std::string& service,
    const std::string& access_key,
    const std::string& secret_key,
    const std::string& payload
) {
    std::string timestamp = get_timestamp();
    std::string date = get_date(timestamp);

    // Payload hash
    std::string payload_hash = sha256(payload);

    // Canonical URI (path)
    std::string canonical_uri = path.empty() ? "/" : path;

    // Canonical query string (already URL-encoded, just sort parameters)
    std::string canonical_query = query;

    // Canonical headers (must be sorted, lowercase)
    std::ostringstream canonical_headers_ss;
    canonical_headers_ss << "host:" << host << "\n";
    canonical_headers_ss << "x-amz-content-sha256:" << payload_hash << "\n";
    canonical_headers_ss << "x-amz-date:" << timestamp << "\n";
    std::string canonical_headers = canonical_headers_ss.str();

    std::string signed_headers = "host;x-amz-content-sha256;x-amz-date";

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
    if (!query.empty()) {
        url_ss << "?" << query;
    }
    result.url = url_ss.str();

    result.headers["Host"] = host;
    result.headers["x-amz-date"] = timestamp;
    result.headers["x-amz-content-sha256"] = payload_hash;
    result.headers["Authorization"] = authorization;

    return result;
}
