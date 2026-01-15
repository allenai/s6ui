import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed


LOG = logging.getLogger("s3_range_list")


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03d %(levelname)s [%(threadName)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_s3_uri(s: str) -> Tuple[str, str]:
    if not s.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {s!r}")
    rest = s[len("s3://"):]
    if not rest or rest.startswith("/"):
        raise ValueError(f"Invalid S3 URI: {s!r}")
    parts = rest.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) == 2 else ""
    return bucket, prefix


_RETRYABLE_CODES = {
    "SlowDown",
    "Throttling",
    "ThrottlingException",
    "RequestLimitExceeded",
    "ServiceUnavailable",
    "InternalError",
}


def _sleep_backoff(attempt: int) -> None:
    base = min(0.25 * (2 ** attempt), 8.0)
    time.sleep(base * (0.5 + random.random()))


def call_list_objects_v2(s3, **kwargs) -> dict:
    for attempt in range(10):
        try:
            return s3.list_objects_v2(**kwargs)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            if code in _RETRYABLE_CODES or status in (429, 500, 503):
                LOG.warning("Retryable S3 error code=%s status=%s attempt=%d", code, status, attempt + 1)
                _sleep_backoff(attempt)
                continue
            raise
    return s3.list_objects_v2(**kwargs)


def list_first_keys(s3, bucket: str, prefix: str, n: int = 1000) -> List[str]:
    # One page max (MaxKeys <= 1000), but keep parameter anyway.
    resp = call_list_objects_v2(s3, Bucket=bucket, Prefix=prefix, MaxKeys=min(1000, n))
    keys = [o["Key"] for o in (resp.get("Contents") or [])]
    return keys[:n]


def list_page_keys(
    s3,
    bucket: str,
    prefix: str,
    start_after: str,
    max_keys: int = 1000,
    high_exclusive: Optional[str] = None,
) -> List[str]:
    resp = call_list_objects_v2(
        s3,
        Bucket=bucket,
        Prefix=prefix,
        StartAfter=start_after,
        MaxKeys=max_keys,
    )
    keys = [o["Key"] for o in (resp.get("Contents") or [])]
    if high_exclusive is None:
        return keys
    return [k for k in keys if k < high_exclusive]


def longest_common_prefix(strings: List[str]) -> str:
    if not strings:
        return ""
    s1 = min(strings)
    s2 = max(strings)
    i = 0
    limit = min(len(s1), len(s2))
    while i < limit and s1[i] == s2[i]:
        i += 1
    return s1[:i]


@dataclass(frozen=True)
class NumericTemplate:
    prefix: str
    literals: List[str]
    widths: List[int]

    @property
    def num_fields(self) -> int:
        return len(self.widths)

    def parse(self, key: str) -> List[int]:
        if not key.startswith(self.prefix):
            raise ValueError("key does not match prefix")
        suffix = key[len(self.prefix):]
        parts = re.split(r"(\d+)", suffix)
        if len(parts) != len(self.literals) + self.num_fields:
            raise ValueError("key does not match template")
        nums: List[int] = []
        for i in range(self.num_fields):
            lit = parts[2 * i]
            num = parts[2 * i + 1]
            if lit != self.literals[i] or len(num) != self.widths[i]:
                raise ValueError("key does not match template")
            nums.append(int(num, 10))
        if parts[-1] != self.literals[-1]:
            raise ValueError("key does not match template")
        return nums

    def build(self, nums: List[int]) -> str:
        out = [self.prefix]
        for i, n in enumerate(nums):
            out.append(self.literals[i])
            out.append(f"{n:0{self.widths[i]}d}")
        out.append(self.literals[-1])
        return "".join(out)

    def jump(self, nums: List[int], field_idx: int) -> Optional[str]:
        next_nums = nums[:]
        next_val = next_nums[field_idx] + 1
        if len(str(next_val)) > self.widths[field_idx]:
            return None
        next_nums[field_idx] = next_val
        for j in range(field_idx + 1, self.num_fields):
            next_nums[j] = 0
        return self.build(next_nums)


def infer_numeric_template(prefix: str, keys: List[str]) -> Optional[NumericTemplate]:
    if not keys:
        return None
    first = keys[0]
    if not first.startswith(prefix):
        return None
    suffix = first[len(prefix):]
    parts = re.split(r"(\d+)", suffix)
    if len(parts) < 3:
        return None
    literals = parts[::2]
    widths = [len(parts[i]) for i in range(1, len(parts), 2)]
    if not widths:
        return None
    for k in keys[1:]:
        if not k.startswith(prefix):
            return None
        sfx = k[len(prefix):]
        p = re.split(r"(\d+)", sfx)
        if len(p) != len(parts):
            return None
        if p[::2] != literals:
            return None
        if [len(p[i]) for i in range(1, len(p), 2)] != widths:
            return None
    return NumericTemplate(prefix=prefix, literals=literals, widths=widths)


def choose_boundary_from_page(
    s3,
    bucket: str,
    prefix: str,
    low_exclusive: str,
    high_exclusive: Optional[str],
    keys: List[str],
) -> Tuple[Optional[str], int]:
    """
    Returns (boundary, peeks_used). Boundary is a key string to split at.
    """
    if len(keys) < 2:
        return None, 0

    peeks_used = 0
    first_key = keys[0]

    lcp = longest_common_prefix(keys)
    if lcp and len(lcp) > len(prefix):
        boundary = peek_next_key(s3, bucket, prefix, start_after=lcp)
        peeks_used += 1
        if boundary is not None:
            if boundary != first_key and boundary > low_exclusive:
                if high_exclusive is None or boundary < high_exclusive:
                    return boundary, peeks_used

    tpl = infer_numeric_template(prefix, keys)
    if tpl is not None:
        nums_list = [tpl.parse(k) for k in keys]
        for idx in range(tpl.num_fields):
            vals = {nums[idx] for nums in nums_list}
            if len(vals) == 1:
                boundary = tpl.jump(nums_list[0], idx)
                if boundary is not None and boundary > low_exclusive:
                    if high_exclusive is None or boundary < high_exclusive:
                        return boundary, peeks_used

    mid = keys[len(keys) // 2]
    if mid == first_key:
        mid = keys[-1]
    if high_exclusive is not None and mid >= high_exclusive:
        mid = None
        for k in reversed(keys):
            if k < high_exclusive and k != first_key:
                mid = k
                break
    return mid, peeks_used


@dataclass(frozen=True)
class HexTemplate:
    # full key is: prefix + dir_prefix + stem + hex(width) + tail
    prefix: str
    dir_prefix: str
    stem: str
    hex_width: int
    tail: str
    lowercase: bool

    def make_key(self, hex_int: int) -> str:
        fmt = f"{{:0{self.hex_width}x}}" if self.lowercase else f"{{:0{self.hex_width}X}}"
        return f"{self.prefix}{self.dir_prefix}{self.stem}{fmt.format(hex_int)}{self.tail}"


_HEX_RE = re.compile(r"^(?P<stem>.*?)(?P<hex>[0-9a-fA-F]{16,})(?P<tail>.*)$")


def infer_hex_template(prefix: str, sample_keys: List[str]) -> Optional[HexTemplate]:
    """
    Tries to infer keys of form:
      <prefix><dir_prefix><stem><hex><tail>
    where <hex> has fixed width and is hex-only.
    """
    if not sample_keys:
        return None

    # Strip prefix, preserve any subdir and basename
    stripped = []
    for k in sample_keys:
        if not k.startswith(prefix):
            continue
        stripped.append(k[len(prefix):])

    if not stripped:
        return None

    # Try match on basename so deeper paths are allowed but must be consistent.
    parsed = []
    for rem in stripped:
        if "/" in rem:
            d, b = rem.rsplit("/", 1)
            dir_prefix = d + "/"
        else:
            dir_prefix = ""
            b = rem

        m = _HEX_RE.match(b)
        if not m:
            return None

        stem = m.group("stem")
        hx = m.group("hex")
        tail = m.group("tail")
        parsed.append((dir_prefix, stem, hx, tail))

    # Require all dir_prefix/stem/tail to be identical across sample (robustness).
    dir0, stem0, _, tail0 = parsed[0]
    for (d, s, _, t) in parsed[1:]:
        if d != dir0 or s != stem0 or t != tail0:
            return None

    # Require fixed hex width
    widths = {len(hx) for (_, _, hx, _) in parsed}
    if len(widths) != 1:
        return None
    width = widths.pop()

    # Determine case preference from sample
    # (If mixed, default to lowercase; it doesn’t matter for StartAfter, but keeps probes closer.)
    is_lower = sum(hx.islower() for (_, _, hx, _) in parsed) >= len(parsed) // 2

    return HexTemplate(
        prefix=prefix,
        dir_prefix=dir0,
        stem=stem0,
        hex_width=width,
        tail=tail0,
        lowercase=is_lower,
    )


def parse_hex_from_key(tpl: HexTemplate, key: str) -> Optional[int]:
    if not key.startswith(tpl.prefix + tpl.dir_prefix + tpl.stem):
        return None
    rest = key[len(tpl.prefix + tpl.dir_prefix + tpl.stem):]
    if len(rest) < tpl.hex_width:
        return None
    hx = rest[:tpl.hex_width]
    if not re.fullmatch(r"[0-9a-fA-F]+", hx):
        return None
    return int(hx, 16)


def peek_next_key(s3, bucket: str, prefix: str, start_after: str) -> Optional[str]:
    resp = call_list_objects_v2(
        s3,
        Bucket=bucket,
        Prefix=prefix,
        StartAfter=start_after,
        MaxKeys=1,
    )
    contents = resp.get("Contents") or []
    return contents[0]["Key"] if contents else None


@dataclass(frozen=True)
class Interval:
    low_exclusive: str             # StartAfter
    high_exclusive: Optional[str]  # stop when key >= high_exclusive
    pivot_inclusive: Optional[str] # include this key once (StartAfter would exclude it)


def discover_intervals_dynamic(
    s3,
    bucket: str,
    prefix: str,
    desired_parts: int,
    max_peeks: int = 2000,
) -> List[Interval]:
    """
    Iterative discovery: sample one page at a time, compute LCP to find a workable
    split boundary, and fall back to mid-key splits when LCP isn't helpful.
    """
    first_key = peek_next_key(s3, bucket, prefix, start_after=prefix)
    if first_key is None:
        return []

    work: List[Interval] = [Interval(low_exclusive=prefix, high_exclusive=None, pivot_inclusive=None)]
    peeks = 0
    stalled_rounds = 0

    while len(work) < desired_parts and peeks < max_peeks:
        iv = work[0]
        keys = list_page_keys(
            s3,
            bucket,
            prefix,
            start_after=iv.low_exclusive,
            high_exclusive=iv.high_exclusive,
        )
        peeks += 1

        if not keys:
            stalled_rounds += 1
            work = work[1:] + [iv]
            if stalled_rounds >= 50:
                LOG.info("Discovery(dynamic): stalled (empty pages). parts=%d peeks=%d", len(work), peeks)
                break
            continue

        boundary, used = choose_boundary_from_page(
            s3=s3,
            bucket=bucket,
            prefix=prefix,
            low_exclusive=iv.low_exclusive,
            high_exclusive=iv.high_exclusive,
            keys=keys,
        )
        peeks += used

        if boundary is None:
            stalled_rounds += 1
            work = work[1:] + [iv]
            if stalled_rounds >= 50:
                LOG.info("Discovery(dynamic): stalled (no boundary). parts=%d peeks=%d", len(work), peeks)
                break
            continue

        right_first = peek_next_key(s3, bucket, prefix, start_after=boundary)
        peeks += 1
        if right_first is None:
            stalled_rounds += 1
            work = work[1:] + [iv]
            if stalled_rounds >= 50:
                LOG.info("Discovery(dynamic): stalled (no right). parts=%d peeks=%d", len(work), peeks)
                break
            continue
        if iv.high_exclusive is not None and right_first >= iv.high_exclusive:
            stalled_rounds += 1
            work = work[1:] + [iv]
            if stalled_rounds >= 50:
                LOG.info("Discovery(dynamic): stalled (right past high). parts=%d peeks=%d", len(work), peeks)
                break
            continue

        left = Interval(
            low_exclusive=iv.low_exclusive,
            high_exclusive=boundary,
            pivot_inclusive=iv.pivot_inclusive,
        )
        right = Interval(
            low_exclusive=boundary,
            high_exclusive=iv.high_exclusive,
            pivot_inclusive=boundary,
        )
        work = [right, left] + work[1:]
        stalled_rounds = 0

        LOG.info(
            "Discovery(dynamic): split parts=%d boundary=%r peeks=%d",
            len(work),
            boundary,
            peeks,
        )

    work.sort(key=lambda x: x.low_exclusive)
    LOG.info("Discovery(dynamic): finished parts=%d peeks=%d", len(work), peeks)
    return work


def discover_intervals_hex(
    s3,
    bucket: str,
    tpl: HexTemplate,
    desired_parts: int,
    max_peeks: int = 2000,
) -> List[Interval]:
    """
    Discovery using probe keys that stay in the same namespace:
      probe = tpl.make_key(mid_int)
      boundary = first real key after probe (MaxKeys=1)
    Split into (.., boundary) and (boundary, ..).
    """
    prefix = tpl.prefix
    # Find first key under prefix once; also anchors that “data exists”.
    first_key = peek_next_key(s3, bucket, prefix, start_after=prefix)
    if first_key is None:
        return []

    first_int = parse_hex_from_key(tpl, first_key)
    if first_int is None:
        raise RuntimeError("Template inference succeeded, but couldn't parse first key with template.")

    LOG.info(
        "Discovery(hex): first_key=%r first_hex=%x tpl=(dir=%r stem=%r width=%d tail=%r)",
        first_key, first_int, tpl.dir_prefix, tpl.stem, tpl.hex_width, tpl.tail
    )

    # Start with one big interval. We keep a numeric search window to choose midpoints.
    # Upper numeric bound starts at full space; we’ll shrink via boundary=None behavior.
    MAX_INT_EXCL = 1 << (4 * tpl.hex_width)  # 16^width

    # Each item: (interval, lo_int, hi_int)
    work: List[Tuple[Interval, int, int]] = [
        (Interval(low_exclusive=prefix, high_exclusive=None, pivot_inclusive=None), first_int, MAX_INT_EXCL)
    ]

    peeks = 0
    stalled_rounds = 0

    while len(work) < desired_parts and peeks < max_peeks:
        # Pick the interval with the biggest numeric span first
        work.sort(key=lambda x: (x[2] - x[1]), reverse=True)
        iv, lo_int, hi_int = work[0]
        span = hi_int - lo_int

        if span <= 1:
            # Can't split numeric window
            stalled_rounds += 1
            if stalled_rounds >= 50:
                LOG.info("Discovery(hex): stalled (too many tiny spans). parts=%d peeks=%d", len(work), peeks)
                break
            # rotate this interval to the end and continue
            work = work[1:] + [(iv, lo_int, hi_int)]
            continue

        # Try to find a boundary that makes BOTH sides non-empty.
        # We'll do a few attempts by probing different mids.
        split_done = False
        attempts = 0
        while attempts < 12 and peeks < max_peeks:
            attempts += 1
            mid_int = (lo_int + hi_int) // 2
            probe = tpl.make_key(mid_int)
            boundary = peek_next_key(s3, bucket, prefix, start_after=probe)
            peeks += 1

            LOG.debug(
                "Discovery(hex): try mid=%x probe=%r -> boundary=%r (lo=%x hi=%x)",
                mid_int, probe, boundary, lo_int, hi_int
            )

            if boundary is None:
                # No keys after this probe => we probed above the max; shrink hi and try again.
                hi_int = mid_int
                continue

            b_int = parse_hex_from_key(tpl, boundary)
            if b_int is None:
                # boundary doesn't match template; abort splitting this way
                LOG.warning("Discovery(hex): boundary doesn't match template: %r", boundary)
                break

            # Ensure left side has something: need some key < boundary within this interval.
            # If boundary is the first key after iv.low_exclusive, then left is empty.
            left_first = peek_next_key(s3, bucket, prefix, start_after=iv.low_exclusive)
            peeks += 1
            if left_first is None or left_first >= boundary:
                # Left empty => move lo up (we are probing too low / interval is already tight)
                lo_int = max(lo_int, b_int)
                continue

            left = (Interval(low_exclusive=iv.low_exclusive, high_exclusive=boundary, pivot_inclusive=iv.pivot_inclusive),
                    lo_int, b_int)
            right = (Interval(low_exclusive=boundary, high_exclusive=iv.high_exclusive, pivot_inclusive=boundary),
                     b_int, hi_int)

            work = [left, right] + work[1:]
            LOG.info(
                "Discovery(hex): split parts=%d boundary=%r b_hex=%x span_left=%d span_right=%d peeks=%d",
                len(work), boundary, b_int, (b_int - lo_int), (hi_int - b_int), peeks
            )
            split_done = True
            stalled_rounds = 0
            break

        if not split_done:
            stalled_rounds += 1
            # rotate interval to end to try others
            work = work[1:] + [(iv, lo_int, hi_int)]
            if stalled_rounds >= 50:
                LOG.info("Discovery(hex): stalled after many failed splits. parts=%d peeks=%d", len(work), peeks)
                break

    intervals = [w[0] for w in work]
    # sort by low_exclusive so concatenation is ordered
    intervals.sort(key=lambda x: x.low_exclusive)
    LOG.info("Discovery(hex): finished parts=%d peeks=%d", len(intervals), peeks)
    return intervals


def list_interval(
    s3,
    bucket: str,
    prefix: str,
    interval: Interval,
    worker_id: int,
    log_every_pages: int = 10,
) -> List[str]:
    keys: List[str] = []
    if interval.pivot_inclusive is not None:
        keys.append(interval.pivot_inclusive)

    token = None
    pages = 0
    last_key = None

    LOG.info("Worker %d: start low=%r high=%r pivot=%r",
             worker_id, interval.low_exclusive, interval.high_exclusive, interval.pivot_inclusive)

    while True:
        kwargs = dict(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
        if token:
            kwargs["ContinuationToken"] = token
        else:
            kwargs["StartAfter"] = interval.low_exclusive

        resp = call_list_objects_v2(s3, **kwargs)
        pages += 1

        contents = resp.get("Contents") or []
        for obj in contents:
            k = obj["Key"]
            last_key = k
            if interval.high_exclusive is not None and k >= interval.high_exclusive:
                LOG.info("Worker %d: hit high at key=%r pages=%d keys=%d", worker_id, k, pages, len(keys))
                return keys
            keys.append(k)

        if pages % log_every_pages == 0:
            LOG.info("Worker %d: pages=%d keys=%d last=%r truncated=%s",
                     worker_id, pages, len(keys), last_key, bool(resp.get("IsTruncated")))

        if not resp.get("IsTruncated"):
            LOG.info("Worker %d: done pages=%d keys=%d last=%r", worker_id, pages, len(keys), last_key)
            return keys

        token = resp["NextContinuationToken"]




def parallel_list_s3_prefix_startafter(
    s3_uri: str,
    desired_parts: int = 64,
    max_workers: int = 32,
    log_level: str = "INFO",
    log_every_pages: int = 10,
    max_peeks: int = 2000,
    sample_n: int = 1000,
) -> List[str]:
    setup_logging(log_level)
    bucket, prefix = parse_s3_uri(s3_uri)

    LOG.info("Start: bucket=%r prefix=%r desired_parts=%d max_workers=%d", bucket, prefix, desired_parts, max_workers)

    cfg = Config(
        retries={"max_attempts": 10, "mode": "adaptive"},
        max_pool_connections=max_workers * 2,
    )
    s3 = boto3.client("s3", config=cfg)

    intervals = discover_intervals_dynamic(
        s3=s3,
        bucket=bucket,
        prefix=prefix,
        desired_parts=desired_parts,
        max_peeks=max_peeks,
    )

    if len(intervals) <= 1 and desired_parts > 1:
        sample = list_first_keys(s3, bucket, prefix, n=sample_n)
        LOG.info("Sample: got %d keys (first=%r)", len(sample), sample[0] if sample else None)
        tpl = infer_hex_template(prefix, sample)
        if tpl is not None:
            intervals = discover_intervals_hex(
                s3=s3,
                bucket=bucket,
                tpl=tpl,
                desired_parts=desired_parts,
                max_peeks=max_peeks,
            )
        else:
            LOG.warning("Template inference failed; using dynamic intervals.")

    if not intervals:
        LOG.info("No keys found under prefix.")
        return []

    results_by_idx: Dict[int, List[str]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(list_interval, s3, bucket, prefix, iv, i, log_every_pages): i
            for i, iv in enumerate(intervals)
        }
        for fut in as_completed(futures):
            i = futures[fut]
            keys = fut.result()
            results_by_idx[i] = keys
            LOG.info("Collect: interval=%d keys=%d", i, len(keys))

    merged: List[str] = []
    seen = set()
    total = 0
    dups = 0
    for i in range(len(intervals)):
        part = results_by_idx.get(i, [])
        for k in part:
            total += 1
            if k in seen:
                dups += 1
                continue
            seen.add(k)
            merged.append(k)

    LOG.info("Done: merged=%d raw=%d dups_dropped=%d", len(merged), total, dups)
    return merged


if __name__ == "__main__":
    keys = parallel_list_s3_prefix_startafter(
        #"s3://ai2-oe-data/jakep/dolma4pdfs_workspaces/olmo-crawled-pdfs_split9_workspace/results/",
        "s3://ai2-oe-data/jakep/dolma4pdfs_frontier/olmo-crawled-pdfs/",
        desired_parts=4,
        max_workers=32,
        log_level="INFO",      # set DEBUG for discovery probe details
        log_every_pages=1,
        max_peeks=2000,
        sample_n=1000,
    )
    print("keys:", len(keys))
