export PYTHONUNBUFFERED=1
# ensure line buffering for anything else you might call
alias run_unbuffered='stdbuf -oL -eL'

# then run the script unbuffered
python3 -u /opt/bq_to_s3.py \
  --gcp-sa-json-path /opt/gcp.json \
  --query-template "{{ QueryTemplate }}" \
  --bq-location "{{ BqLocation }}" \
  --s3-bucket "{{ S3Bucket }}" \
  --s3-prefix "${PREFIX_BASE}" \
  --max-workers 6 \
  --retries {{ Retries }} \
  --retry-backoff-seconds {{ RetryBackoffSeconds }} \
  --gzip \
  --multipart-chunk-mib 128 \
  --multipart-max-concurrency 16
