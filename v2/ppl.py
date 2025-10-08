# parse-progress-lines (Python 3.11)
import json, re

PAT = re.compile(r"^PROGRESS\s+([0-9.]+|NA)\s+([0-9]+)\s*$", re.MULTILINE)

def lambda_handler(event, _ctx):
    out = event.get("stdout") or ""
    matches = PAT.findall(out)
    if not matches:
        return {"percent": None, "rows": None, "found": False}
    pct, rows = matches[-1]
    return {
        "percent": None if pct == "NA" else float(pct),
        "rows": int(rows),
        "found": True
    }