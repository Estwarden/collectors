"""Google Cloud API client — flat functions using google-auth library.

Uses Application Default Credentials or GOOGLE_APPLICATION_CREDENTIALS.
"""
import base64
import json
import urllib.error
import urllib.parse
import urllib.request

import google.auth
import google.auth.transport.requests

_credentials = None
_auth_request = google.auth.transport.requests.Request()


def _get_creds():
    global _credentials
    if _credentials is None or not _credentials.valid:
        _credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
    if not _credentials.valid:
        _credentials.refresh(_auth_request)
    return _credentials


def _auth_headers():
    creds = _get_creds()
    return {"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"}


def _post(url, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=_auth_headers(), method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def _get(url):
    req = urllib.request.Request(url, headers=_auth_headers())
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


# ── Public API ──

def translate(text, target="en", source=None):
    body = {"q": text, "target": target, "format": "text"}
    if source:
        body["source"] = source
    resp = _post("https://translation.googleapis.com/language/translate/v2", body)
    translations = resp.get("data", {}).get("translations", [])
    return translations[0].get("translatedText", text) if translations else text


def detect_language(text):
    resp = _post("https://translation.googleapis.com/language/translate/v2/detect", {"q": text})
    detections = resp.get("data", {}).get("detections", [[]])
    return detections[0][0].get("language", "unknown") if detections and detections[0] else "unknown"


def extract_entities(text, language=None):
    doc = {"type": "PLAIN_TEXT", "content": text}
    if language:
        doc["language"] = language
    resp = _post("https://language.googleapis.com/v1/documents:analyzeEntities",
                 {"document": doc, "encodingType": "UTF8"})
    return [{"name": e.get("name", ""), "type": e.get("type", ""),
             "salience": e.get("salience", 0), "mentions": len(e.get("mentions", [])),
             "metadata": e.get("metadata", {})} for e in resp.get("entities", [])]


def analyze_sentiment(text):
    resp = _post("https://language.googleapis.com/v1/documents:analyzeSentiment",
                 {"document": {"type": "PLAIN_TEXT", "content": text}, "encodingType": "UTF8"})
    s = resp.get("documentSentiment", {})
    return {"score": s.get("score", 0), "magnitude": s.get("magnitude", 0)}


def geocode(address):
    params = urllib.parse.urlencode({"address": address})
    resp = _get(f"https://maps.googleapis.com/maps/api/geocode/json?{params}")
    results = resp.get("results", [])
    if results:
        loc = results[0].get("geometry", {}).get("location", {})
        return {"lat": loc.get("lat"), "lng": loc.get("lng"),
                "formatted": results[0].get("formatted_address", "")}
    return None


def ocr_image(image_bytes):
    b64 = base64.b64encode(image_bytes).decode()
    resp = _post("https://vision.googleapis.com/v1/images:annotate",
                 {"requests": [{"image": {"content": b64}, "features": [{"type": "TEXT_DETECTION"}]}]})
    annotations = resp.get("responses", [{}])[0].get("textAnnotations", [])
    return annotations[0].get("description", "") if annotations else ""
