"""Google Cloud API client for pipeline DAGs.

Wraps Translation, Natural Language, Vision, Geocoding, and TTS.
Uses service account key from GOOGLE_APPLICATION_CREDENTIALS env var.

Usage:
    from google_client import GoogleClient
    gc = GoogleClient()
    translated = gc.translate("Российские войска...", target="en")
    entities = gc.extract_entities("Estonian border patrol reported...")
    coords = gc.geocode("Kaliningrad port")
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error

# Google auth via service account JWT
# We use the REST API directly — no google-cloud-* pip packages needed.
# Auth: get access token from service account key via JWT → OAuth2 token exchange.

import base64
import hashlib
import hmac


def _b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _get_access_token(key_path=None):
    """Get OAuth2 access token from service account JSON key."""
    key_path = key_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not key_path or not os.path.exists(key_path):
        raise ValueError(f"GOOGLE_APPLICATION_CREDENTIALS not set or file missing: {key_path}")

    with open(key_path) as f:
        sa = json.load(f)

    # Build JWT
    now = int(time.time())
    header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    payload = _b64url(json.dumps({
        "iss": sa["client_email"],
        "scope": "https://www.googleapis.com/auth/cloud-platform",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }).encode())

    # Sign with private key
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    private_key = serialization.load_pem_private_key(
        sa["private_key"].encode(), password=None
    )
    signature = private_key.sign(
        f"{header}.{payload}".encode(),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    jwt = f"{header}.{payload}.{_b64url(signature)}"

    # Exchange JWT for access token
    data = urllib.parse.urlencode({
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt,
    }).encode()
    req = urllib.request.Request("https://oauth2.googleapis.com/token", data=data)
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["access_token"]


# Cache token for reuse within a DAG run
_token_cache = {"token": None, "expires": 0}
import urllib.parse


def _auth_headers():
    now = time.time()
    if _token_cache["token"] and _token_cache["expires"] > now + 60:
        token = _token_cache["token"]
    else:
        token = _get_access_token()
        _token_cache["token"] = token
        _token_cache["expires"] = now + 3500
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


class GoogleClient:
    """Lightweight Google Cloud API client using REST + service account auth."""

    # ── Translation ──

    def translate(self, text, target="en", source=None):
        """Translate text. Returns translated string."""
        body = {"q": text, "target": target, "format": "text"}
        if source:
            body["source"] = source
        resp = self._post(
            "https://translation.googleapis.com/language/translate/v2",
            body,
        )
        translations = resp.get("data", {}).get("translations", [])
        if translations:
            return translations[0].get("translatedText", text)
        return text

    def detect_language(self, text):
        """Detect language. Returns language code (e.g., 'ru', 'et')."""
        resp = self._post(
            "https://translation.googleapis.com/language/translate/v2/detect",
            {"q": text},
        )
        detections = resp.get("data", {}).get("detections", [[]])
        if detections and detections[0]:
            return detections[0][0].get("language", "unknown")
        return "unknown"

    # ── Natural Language ──

    def extract_entities(self, text, language=None):
        """Extract entities (persons, orgs, locations, events). Returns list of dicts."""
        doc = {"type": "PLAIN_TEXT", "content": text}
        if language:
            doc["language"] = language
        resp = self._post(
            "https://language.googleapis.com/v1/documents:analyzeEntities",
            {"document": doc, "encodingType": "UTF8"},
        )
        entities = []
        for e in resp.get("entities", []):
            entities.append({
                "name": e.get("name", ""),
                "type": e.get("type", ""),
                "salience": e.get("salience", 0),
                "mentions": len(e.get("mentions", [])),
                "metadata": e.get("metadata", {}),
            })
        return entities

    def analyze_sentiment(self, text):
        """Analyze sentiment. Returns {"score": -1..1, "magnitude": 0..inf}."""
        resp = self._post(
            "https://language.googleapis.com/v1/documents:analyzeSentiment",
            {"document": {"type": "PLAIN_TEXT", "content": text}, "encodingType": "UTF8"},
        )
        s = resp.get("documentSentiment", {})
        return {"score": s.get("score", 0), "magnitude": s.get("magnitude", 0)}

    # ── Geocoding ──

    def geocode(self, address):
        """Geocode an address/place name. Returns {"lat": float, "lng": float, "formatted": str} or None."""
        params = urllib.parse.urlencode({"address": address})
        resp = self._get(f"https://maps.googleapis.com/maps/api/geocode/json?{params}")
        results = resp.get("results", [])
        if results:
            loc = results[0].get("geometry", {}).get("location", {})
            return {
                "lat": loc.get("lat"),
                "lng": loc.get("lng"),
                "formatted": results[0].get("formatted_address", ""),
            }
        return None

    # ── Vision ──

    def ocr_image(self, image_bytes):
        """Extract text from image bytes. Returns string."""
        b64 = base64.b64encode(image_bytes).decode()
        resp = self._post(
            "https://vision.googleapis.com/v1/images:annotate",
            {"requests": [{"image": {"content": b64},
                           "features": [{"type": "TEXT_DETECTION"}]}]},
        )
        annotations = resp.get("responses", [{}])[0].get("textAnnotations", [])
        if annotations:
            return annotations[0].get("description", "")
        return ""

    def label_image(self, image_bytes):
        """Detect labels in image. Returns list of {"label": str, "score": float}."""
        b64 = base64.b64encode(image_bytes).decode()
        resp = self._post(
            "https://vision.googleapis.com/v1/images:annotate",
            {"requests": [{"image": {"content": b64},
                           "features": [{"type": "LABEL_DETECTION", "maxResults": 10}]}]},
        )
        labels = resp.get("responses", [{}])[0].get("labelAnnotations", [])
        return [{"label": l["description"], "score": l["score"]} for l in labels]

    # ── Internal ──

    def _post(self, url, body):
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, headers=_auth_headers(), method="POST")
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())

    def _get(self, url):
        req = urllib.request.Request(url, headers=_auth_headers())
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
