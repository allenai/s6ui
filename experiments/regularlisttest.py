#!/usr/bin/env python3
"""
Simple S3 bucket listing script that times each request chunk/continuation token.
Usage: python regularlisttest.py s3://bucket/prefix/
"""

import sys
import time
import boto3
from urllib.parse import urlparse


def parse_s3_url(s3_url):
    """Parse an S3 URL into bucket and prefix."""
    parsed = urlparse(s3_url)
    if parsed.scheme != 's3':
        raise ValueError(f"Expected s3:// URL, got: {s3_url}")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip('/')
    return bucket, prefix


def list_s3_objects(s3_url):
    """List all objects in an S3 bucket/prefix, timing each request."""
    bucket, prefix = parse_s3_url(s3_url)

    print(f"Listing s3://{bucket}/{prefix}")
    print(f"{'Request #':<12} {'Objects':<12} {'Time (s)':<12} {'Total Objects':<15} {'Continuation Token'}")
    print("-" * 80)

    s3 = boto3.client('s3')

    request_num = 0
    total_objects = 0
    continuation_token = None
    total_time = 0

    while True:
        request_num += 1

        # Build request parameters
        params = {
            'Bucket': bucket,
            'MaxKeys': 1000,
        }
        if prefix:
            params['Prefix'] = prefix
        if continuation_token:
            params['ContinuationToken'] = continuation_token

        # Time the request
        start_time = time.perf_counter()
        response = s3.list_objects_v2(**params)
        elapsed = time.perf_counter() - start_time
        total_time += elapsed

        # Count objects in this response
        objects_in_response = len(response.get('Contents', []))
        total_objects += objects_in_response

        # Get continuation token info
        is_truncated = response.get('IsTruncated', False)
        next_token = response.get('NextContinuationToken', '')
        token_preview = next_token[:30] + '...' if len(next_token) > 30 else next_token

        print(f"{request_num:<12} {objects_in_response:<12} {elapsed:<12.3f} {total_objects:<15} {token_preview}")

        # Check if there are more results
        if not is_truncated:
            break

        continuation_token = next_token

    print("-" * 80)
    print(f"Summary: {total_objects} objects listed in {request_num} requests, total time: {total_time:.3f}s")
    print(f"Average time per request: {total_time/request_num:.3f}s")


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} s3://bucket/prefix/")
        print(f"Example: {sys.argv[0]} s3://my-bucket/some/prefix/")
        sys.exit(1)

    s3_url = sys.argv[1]

    try:
        list_s3_objects(s3_url)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
