# file: lambda_function.py
# runtime: python3.12

import os
import json
from datetime import datetime, timezone
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

def _head_object_safe(bucket: str, key: str) -> Dict[str, Any] | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code == 404:
            return None
        # treat other errors as "missing" but include a log line
        print(f"[warn] head_object error for s3://{bucket}/{key}: {e}")
        return None

def _put_manifest(bucket: str, key: str, manifest: Dict[str, Any]) -> None:
    body = json.dumps(manifest, ensure_ascii=False, separators=(",", ":"))
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-store",
    )

def _expected_keys(prefix_base: str, parts: int, ext: str) -> List[str]:
    # ext should include leading dot (".csv" or ".csv.gz")
    return [f"{prefix_base}_part_{i:02d}{ext}" for i in range(1, parts + 1)]

def lambda_handler(event, context):
    """
    Event fields (all optional if provided via env vars):
      bucket            : S3 bucket name (env: BUCKET)
      prefix_base       : e.g. DATA_SOURCE_2025_10_08 (no _part_xx or extension) (env: PREFIX_BASE)
      extension         : ".csv" or ".csv.gz" (env: EXTENSION)  [default ".csv"]
      expected_parts    : integer [default 12] (env: EXPECTED_PARTS)
      require_nonzero   : bool [default true] (env: REQUIRE_NONZERO)
      write_manifest    : bool [default true] (env: WRITE_MANIFEST)
      manifest_key      : where to write manifest [default f"{prefix_base}_manifest.json"] (env: MANIFEST_KEY)
    Returns:
      { ok: bool, missing: [ints], empty: [ints], manifestKey: "...", files: [...], totalSize: int }
    """
    bucket       = event.get("bucket")       or _env("BUCKET")
    prefix_base  = event.get("prefix_base")  or _env("PREFIX_BASE")
    extension    = event.get("extension")    or _env("EXTENSION", ".csv")
    expected     = int(event.get("expected_parts") or _env("EXPECTED_PARTS", "12"))
    require_nz   = str(event.get("require_nonzero") if "require_nonzero" in event else _env("REQUIRE_NONZERO", "true")).lower() == "true"
    write_man    = str(event.get("write_manifest")  if "write_manifest"  in event else _env("WRITE_MANIFEST", "true")).lower() == "true"
    manifest_key = event.get("manifest_key") or _env("MANIFEST_KEY") or f"{prefix_base}_manifest.json"

    if not bucket or not prefix_base:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "bucket and prefix_base are required (event or env)"})
        }

    if not extension.startswith("."):
        extension = "." + extension

    keys = _expected_keys(prefix_base, expected, extension)

    files = []
    missing_months = []
    empty_months = []
    total_size = 0

    # check each part
    for idx, key in enumerate(keys, start=1):
        head = _head_object_safe(bucket, key)
        if head is None:
            missing_months.append(idx)
            continue

        size = int(head.get("ContentLength", 0))
        lm = head.get("LastModified")
        etag = head.get("ETag", "").strip('"')
        checksum = head.get("ChecksumSHA256") or head.get("ChecksumCRC32C") or None  # present only if provided at upload

        files.append({
            "month_id": idx,
            "key": key,
            "size": size,
            "etag": etag,
            "checksum": checksum,
            "last_modified": lm.isoformat() if lm else None
        })
        total_size += size
        if size == 0:
            empty_months.append(idx)

    ok = (len(missing_months) == 0) and (not require_nz or len(empty_months) == 0)

    manifest = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "COMPLETE" if ok else "INCOMPLETE",
        "bucket": bucket,
        "base_prefix": prefix_base,
        "extension": extension,
        "expected_parts": expected,
        "require_nonzero": require_nz,
        "total_size": total_size,
        "ok": ok,
        "missing_months": missing_months,
        "empty_months": empty_months,
        "files": files
    }

    if write_man:
        _put_manifest(bucket, manifest_key, manifest)
        print(f"wrote manifest to s3://{bucket}/{manifest_key}")

    # tiny CloudWatch metric (Embedded Metric Format)
    try:
        metric_doc = {
            "_aws": {
                "Timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                "CloudWatchMetrics": [{
                    "Namespace": "BQExport",
                    "Dimensions": [["Bucket", "BasePrefix"]],
                    "Metrics": [
                        {"Name": "PartsPresent", "Unit": "Count"},
                        {"Name": "PartsMissing", "Unit": "Count"},
                        {"Name": "PartsEmpty", "Unit": "Count"}
                    ]
                }]
            },
            "Bucket": bucket,
            "BasePrefix": prefix_base,
            "PartsPresent": len(files),
            "PartsMissing": len(missing_months),
            "PartsEmpty": len(empty_months),
        }
        print(json.dumps(metric_doc))
    except Exception as e:
        print(f"[warn] failed to emit EMF metrics: {e}")

    return {
        "statusCode": 200 if ok else 500,
        "body": json.dumps({
            "ok": ok,
            "bucket": bucket,
            "prefix_base": prefix_base,
            "manifestKey": manifest_key if write_man else None,
            "missing": missing_months,
            "empty": empty_months,
            "totalSize": total_size,
            "files": files
        })
    }