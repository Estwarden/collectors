"""EstWarden Data API client — flat functions, no classes."""
import json
import os
import sys
import urllib.request
import urllib.error

_redis_client = None

def _get_redis():
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    try:
        import redis as _redis_mod
        url = os.environ.get("REDIS_URL", "redis://redis:6379")
        _redis_client = _redis_mod.from_url(url, socket_timeout=5)
        _redis_client.ping()
        return _redis_client
    except Exception as e:
        print(f"Queue mode Redis init failed: {e}", file=sys.stderr)
        _redis_client = False
        return None

def _api_base():
    base = os.environ.get("ESTWARDEN_API_URL", "").rstrip("/")
    if not base:
        raise ValueError("ESTWARDEN_API_URL not set")
    return base

def _api_key():
    key = os.environ.get("ESTWARDEN_API_KEY", "")
    if not key:
        raise ValueError("ESTWARDEN_API_KEY not set")
    return key

def _is_queue_mode():
    return os.environ.get("ESTWARDEN_QUEUE_MODE", "") in ("1", "true", "yes")

def _api_post(path, body, timeout=30):
    data = json.dumps(body).encode()
    url = f"{_api_base()}/{path.lstrip('/')}"
    req = urllib.request.Request(
        url, data=data,
        headers={"X-Pipeline-Key": _api_key(), "Content-Type": "application/json"},
        method="POST",
    )
    return _api_do(req, timeout)

def _api_get(path):
    url = f"{_api_base()}/{path.lstrip('/')}"
    req = urllib.request.Request(url, headers={"X-Pipeline-Key": _api_key()})
    return _api_do(req)

def _api_do(req, timeout=30):
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f"API error {e.code}: {e.read().decode()[:500]}", file=sys.stderr)
        raise
    except Exception as e:
        print(f"API request failed: {e}", file=sys.stderr)
        raise


# ── Public API ──

def ingest_signals(signals: list) -> dict:
    if _is_queue_mode():
        rdb = _get_redis()
        if rdb:
            batch = json.dumps({"pipeline_key": _api_key(), "signals": signals})
            rdb.lpush("ingest:queue:signals", batch)
            return {"queued": len(signals)}
        print("Queue mode fallback to HTTP", file=sys.stderr)
    return _api_post("/api/v1/ingest/signals", {"signals": signals})

def ingest_tags(tags: list) -> dict:
    return _api_post("/api/v1/ingest/narrative-tags", {"tags": tags})

def ingest_campaigns(campaigns: list) -> dict:
    return _api_post("/api/v1/ingest/campaigns", {"campaigns": campaigns})

def ingest_anomalies(anomalies: list) -> dict:
    return _api_post("/api/v1/ingest/anomalies", {"anomalies": anomalies})

def ingest_threat_index(date: str, score: float, level: str, region: str = "baltic",
                        components: dict = None, details: dict = None) -> dict:
    payload = {"date": date, "score": score, "level": level, "region": region}
    if components:
        payload["components"] = components
    if details:
        payload["details"] = details
    return _api_post("/api/v1/ingest/threat-index", payload)

def query_signals(source_type=None, since="24h", limit=100) -> list:
    params = f"since={since}&limit={limit}"
    if source_type:
        params += f"&source_type={source_type}"
    return _api_get(f"/api/v1/query/signals?{params}").get("signals", [])

def query_untagged(source_types=None, limit=30) -> list:
    params = f"limit={limit}"
    if source_types:
        params += f"&source_types={','.join(source_types)}"
    return _api_get(f"/api/v1/query/untagged?{params}").get("signals", [])

def query_report(date: str) -> dict:
    return _api_get(f"/api/v1/query/report/{date}")

def query_baselines(region: str = None) -> list:
    params = f"?region={region}" if region else ""
    return _api_get(f"/api/v1/query/baselines{params}").get("baselines", [])

def query_cti_input() -> dict:
    return _api_get("/api/v1/query/cti-input")

def query_report_data(region: str = "baltic") -> dict:
    return _api_get(f"/api/v1/query/report-data?region={region}")

def write_report(date: str, threat_level: str, raw_intel: str, summary: str,
                 cti_score: float, cti_level: str, cti_trend: str,
                 indicators: list = None, cti_components: dict = None) -> dict:
    payload = {"date": date, "threat_level": threat_level, "raw_intel": raw_intel,
               "summary": summary, "cti_score": cti_score, "cti_level": cti_level,
               "cti_trend": cti_trend}
    if indicators:
        payload["indicators"] = indicators
    if cti_components:
        payload["cti_components"] = cti_components
    return _api_post("/api/v1/process/write-report", payload)

def detect_campaigns() -> dict:
    return _api_post("/api/v1/detect/campaigns", {})
