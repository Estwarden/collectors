"""EstWarden Data API client for pipeline collectors.

Usage:
    from estwarden_client import EstWardenClient
    client = EstWardenClient()  # reads ESTWARDEN_API_URL + ESTWARDEN_API_KEY from env
    result = client.ingest_signals([{...}, ...])
"""

import json
import os
import sys
import urllib.request
import urllib.error


class EstWardenClient:
    def __init__(self, base_url=None, api_key=None):
        self.base = (base_url or os.environ.get("ESTWARDEN_API_URL", "")).rstrip("/")
        self.key = api_key or os.environ.get("ESTWARDEN_API_KEY", "")
        if not self.base:
            raise ValueError("ESTWARDEN_API_URL not set")
        if not self.key:
            raise ValueError("ESTWARDEN_API_KEY not set")

    # ── Ingest ──

    def ingest_signals(self, signals: list) -> dict:
        """Submit signals. Returns {"inserted": N, "duplicates": N, "errors": [...]}"""
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

    # ── Detection ──

    def detect_campaigns(self) -> dict:
        """Trigger server-side campaign detection. Returns {"resolved": N, "created": N, "campaigns": [...]}"""
        return self._post("/api/v1/detect/campaigns", {})

    # ── Internal ──

    def _post(self, path, body):
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            f"{self.base}{path}",
            data=data,
            headers={
                "X-Pipeline-Key": self.key,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        return self._do(req)

    def _get(self, path):
        req = urllib.request.Request(
            f"{self.base}{path}",
            headers={"X-Pipeline-Key": self.key},
        )
        return self._do(req)

    def _do(self, req):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:500]
            print(f"API error {e.code}: {body}", file=sys.stderr)
            raise
        except Exception as e:
            print(f"API request failed: {e}", file=sys.stderr)
            raise
