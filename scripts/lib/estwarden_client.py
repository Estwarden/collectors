"""EstWarden Data API client for pipeline collectors.

Usage:
    from estwarden_client import EstWardenClient
    client = EstWardenClient()  # reads ESTWARDEN_API_URL + ESTWARDEN_API_KEY from env
    result = client.ingest_signals([{...}, ...])

Queue mode (optional):
    Set ESTWARDEN_QUEUE_MODE=1 and REDIS_URL=redis://redis:6379
    to LPUSH signal batches to Redis instead of HTTP POST.
    The ingest service BRPOP-s and inserts at its own pace.
"""

import json
import os
import sys
import urllib.request
import urllib.error

_redis_client = None


def _get_redis():
    """Lazy-init Redis connection for queue mode."""
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
        _redis_client = False  # sentinel: don't retry
        return None


class EstWardenClient:
    def __init__(self, base_url=None, api_key=None):
        self.base = (base_url or os.environ.get("ESTWARDEN_API_URL", "")).rstrip("/")
        self.key = api_key or os.environ.get("ESTWARDEN_API_KEY", "")
        self.queue_mode = os.environ.get("ESTWARDEN_QUEUE_MODE", "") in ("1", "true", "yes")
        if not self.base and not self.queue_mode:
            raise ValueError("ESTWARDEN_API_URL not set")
        if not self.key:
            raise ValueError("ESTWARDEN_API_KEY not set")

    # ── Ingest ──

    def ingest_signals(self, signals: list) -> dict:
        """Submit signals. Uses Redis queue if ESTWARDEN_QUEUE_MODE=1, else HTTP POST."""
        if self.queue_mode:
            rdb = _get_redis()
            if rdb:
                batch = json.dumps({"pipeline_key": self.key, "signals": signals})
                rdb.lpush("ingest:queue:signals", batch)
                return {"queued": len(signals)}
            # Fallback to HTTP if Redis unavailable
            print("Queue mode fallback to HTTP", file=sys.stderr)
        return self._post("/api/v1/ingest/signals", {"signals": signals})

    def ingest_tags(self, tags: list) -> dict:
        """Submit narrative tags. Returns {"inserted": N, "skipped": N}"""
        return self._post("/api/v1/ingest/narrative-tags", {"tags": tags})

    def ingest_campaigns(self, campaigns: list) -> dict:
        """Submit detected campaigns. Returns {"created": N}"""
        return self._post("/api/v1/ingest/campaigns", {"campaigns": campaigns})

    def ingest_anomalies(self, anomalies: list) -> dict:
        """Submit anomaly events. Returns {"created": N}"""
        return self._post("/api/v1/ingest/anomalies", {"anomalies": anomalies})

    def ingest_threat_index(self, date: str, score: float, level: str, region: str = "baltic",
                            components: dict = None, details: dict = None) -> dict:
        """Update threat index for a date. Region defaults to 'baltic'."""
        payload = {"date": date, "score": score, "level": level, "region": region}
        if components:
            payload["components"] = components
        if details:
            payload["details"] = details
        return self._post("/api/v1/ingest/threat-index", payload)

    # ── Query ──

    def query_signals(self, source_type=None, since="24h", limit=100) -> list:
        """Get recent signals. Returns list of signal dicts."""
        params = f"since={since}&limit={limit}"
        if source_type:
            params += f"&source_type={source_type}"
        resp = self._get(f"/api/v1/query/signals?{params}")
        return resp.get("signals", [])

    def query_untagged(self, source_types=None, limit=30) -> list:
        """Get signals needing classification."""
        params = f"limit={limit}"
        if source_types:
            params += f"&source_types={','.join(source_types)}"
        resp = self._get(f"/api/v1/query/untagged?{params}")
        return resp.get("signals", [])

    def query_report(self, date: str) -> dict:
        """Get daily report data for briefing."""
        return self._get(f"/api/v1/query/report/{date}")

    def query_baselines(self, region: str = None) -> list:
        """Get 7-day rolling baselines per source type. Optional region filter (comma-separated)."""
        params = ""
        if region:
            params = f"?region={region}"
        resp = self._get(f"/api/v1/query/baselines{params}")
        return resp.get("baselines", [])

    # ── CTI + Report ──

    def query_cti_input(self) -> dict:
        """Get all data needed for CTI computation in one call."""
        return self._get("/api/v1/query/cti-input")

    def query_report_data(self, region: str = "baltic") -> dict:
        """Get all data needed for daily report generation."""
        return self._get(f"/api/v1/query/report-data?region={region}")

    def write_report(self, date: str, threat_level: str, raw_intel: str, summary: str,
                     cti_score: float, cti_level: str, cti_trend: str,
                     indicators: list = None) -> dict:
        """Write or update a daily report with indicators."""
        payload = {
            "date": date, "threat_level": threat_level,
            "raw_intel": raw_intel, "summary": summary,
            "cti_score": cti_score, "cti_level": cti_level, "cti_trend": cti_trend,
        }
        if indicators:
            payload["indicators"] = indicators
        return self._post("/api/v1/process/write-report", payload)

    # ── Detection ──

    def detect_campaigns(self) -> dict:
        """Trigger server-side campaign detection. Returns {"resolved": N, "created": N, "campaigns": [...]}"""
        return self._post("/api/v1/detect/campaigns", {})

    # ── Internal ──

    def _post(self, path, body, timeout=30):
        data = json.dumps(body).encode()
        url = f"{self.base}/{path.lstrip('/')}"
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "X-Pipeline-Key": self.key,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        return self._do(req, timeout=timeout)

    def _get(self, path):
        url = f"{self.base}/{path.lstrip('/')}"
        req = urllib.request.Request(
            url,
            headers={"X-Pipeline-Key": self.key},
        )
        return self._do(req)

    def _do(self, req, timeout=30):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:500]
            print(f"API error {e.code}: {body}", file=sys.stderr)
            raise
        except Exception as e:
            print(f"API request failed: {e}", file=sys.stderr)
            raise
