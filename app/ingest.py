from __future__ import annotations

import re
import time
from typing import Any


REDACTION_PATTERNS = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "phone": re.compile(r"(?<!\d)(?:\+?\d[\d .()-]{7,}\d)(?!\d)"),
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "card": re.compile(r"\b(?:\d[ -]*?){13,19}\b"),
}

REQUIRED_FIELDS = {
    "event_id",
    "conversation_id",
    "provider",
    "model",
    "started_at",
    "ended_at",
    "latency_ms",
    "status",
}


def now_ms() -> int:
    return int(time.time() * 1000)


def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, len(text.split()) + len(text) // 18)


def redact_text(text: str) -> tuple[str, dict[str, int]]:
    redacted = text or ""
    counts: dict[str, int] = {}
    for name, pattern in REDACTION_PATTERNS.items():
        redacted, count = pattern.subn(f"[REDACTED_{name.upper()}]", redacted)
        counts[name] = count
    return redacted, counts


def preview(text: str, limit: int = 320) -> str:
    cleaned, _ = redact_text(" ".join((text or "").split()))
    return cleaned[:limit]


def validate_inference_event(payload: dict[str, Any]) -> dict[str, Any]:
    missing = sorted(REQUIRED_FIELDS - set(payload))
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")

    status = payload.get("status")
    if status not in {"success", "error", "cancelled"}:
        raise ValueError("status must be success, error, or cancelled")

    normalized = dict(payload)
    normalized["latency_ms"] = int(payload.get("latency_ms") or 0)
    normalized["prompt_tokens"] = int(payload.get("prompt_tokens") or 0)
    normalized["completion_tokens"] = int(payload.get("completion_tokens") or 0)
    normalized["total_tokens"] = int(payload.get("total_tokens") or 0)
    normalized["metadata"] = dict(payload.get("metadata") or {})
    return normalized


def extract_metadata(event: dict[str, Any]) -> dict[str, Any]:
    input_preview = event.get("input_preview") or ""
    output_preview = event.get("output_preview") or ""
    combined = f"{input_preview}\n{output_preview}"
    _, redaction_counts = redact_text(combined)
    metadata = dict(event.get("metadata") or {})
    metadata.update(
        {
            "input_preview_chars": len(input_preview),
            "output_preview_chars": len(output_preview),
            "redaction_counts": redaction_counts,
            "has_error": event.get("status") == "error",
            "is_streaming": bool(metadata.get("stream")),
        }
    )
    return metadata
