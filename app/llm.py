from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Generator

from app.ingest import estimate_tokens, now_ms, preview


Message = dict[str, str]


@dataclass
class StreamResult:
    chunks: Generator[str, None, None]
    event_id: str


class ProviderError(Exception):
    pass


class LLMClient:
    def __init__(
        self,
        provider: str,
        model: str,
        ingestion_url: str = "",
        timeout_seconds: int = 60,
        ingestion_sink: Callable[[dict[str, Any]], None] | None = None,
    ):
        self.provider = provider
        self.model = model
        self.ingestion_url = ingestion_url
        self.timeout_seconds = timeout_seconds
        self.ingestion_sink = ingestion_sink

    def stream_chat(
        self,
        conversation_id: str,
        messages: list[Message],
        cancel_check: Callable[[], bool] | None = None,
    ) -> StreamResult:
        event_id = str(uuid.uuid4())

        def run() -> Generator[str, None, None]:
            started = now_ms()
            output_parts: list[str] = []
            status = "success"
            error = None
            prompt_text = "\n".join(f"{m['role']}: {m['content']}" for m in messages)
            try:
                for chunk in self._provider_stream(messages):
                    if cancel_check and cancel_check():
                        status = "cancelled"
                        break
                    output_parts.append(chunk)
                    yield chunk
            except Exception as exc:
                status = "error"
                error = str(exc)
                raise
            finally:
                ended = now_ms()
                output_text = "".join(output_parts)
                prompt_tokens = estimate_tokens(prompt_text)
                completion_tokens = estimate_tokens(output_text)
                event = {
                    "event_id": event_id,
                    "conversation_id": conversation_id,
                    "provider": self.provider,
                    "model": self.model,
                    "started_at": started,
                    "ended_at": ended,
                    "latency_ms": max(0, ended - started),
                    "status": status,
                    "error": error,
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": prompt_tokens + completion_tokens,
                    "input_preview": preview(prompt_text),
                    "output_preview": preview(output_text),
                    "metadata": {
                        "stream": True,
                        "context_message_count": len(messages),
                    },
                }
                self._send_ingestion_async(event)

        return StreamResult(chunks=run(), event_id=event_id)

    def _provider_stream(self, messages: list[Message]) -> Generator[str, None, None]:
        provider = self.provider.lower()
        if provider == "mock":
            yield from self._mock_stream(messages)
        elif provider == "openai":
            yield from self._openai_stream(messages)
        elif provider == "anthropic":
            yield from self._anthropic_stream(messages)
        elif provider == "gemini":
            yield from self._gemini_stream(messages)
        else:
            raise ProviderError(f"unsupported provider: {self.provider}")

    def _mock_stream(self, messages: list[Message]) -> Generator[str, None, None]:
        user_messages = [m["content"] for m in messages if m["role"] == "user"]
        latest = user_messages[-1] if user_messages else ""
        response = (
            "Mock response: I received your message and kept the recent conversation context. "
            f"You said: {latest[:180]}"
        )
        for word in response.split(" "):
            time.sleep(0.035)
            yield word + " "

    def _openai_stream(self, messages: list[Message]) -> Generator[str, None, None]:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ProviderError("OPENAI_API_KEY is not set")
        payload = {"model": self.model, "messages": messages, "stream": True}
        req = urllib.request.Request(
            "https://api.openai.com/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        yield from self._read_openai_sse(req)

    def _anthropic_stream(self, messages: list[Message]) -> Generator[str, None, None]:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ProviderError("ANTHROPIC_API_KEY is not set")
        system = "\n".join(m["content"] for m in messages if m["role"] == "system")
        anthropic_messages = [m for m in messages if m["role"] in {"user", "assistant"}]
        payload = {
            "model": self.model,
            "max_tokens": 800,
            "system": system,
            "messages": anthropic_messages,
            "stream": True,
        }
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
            for raw in resp:
                line = raw.decode("utf-8").strip()
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    break
                event = json.loads(data)
                delta = event.get("delta") or {}
                if delta.get("type") == "text_delta" and delta.get("text"):
                    yield delta["text"]

    def _gemini_stream(self, messages: list[Message]) -> Generator[str, None, None]:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ProviderError("GEMINI_API_KEY is not set")
        contents = []
        for message in messages:
            if message["role"] == "system":
                continue
            role = "model" if message["role"] == "assistant" else "user"
            contents.append({"role": role, "parts": [{"text": message["content"]}]})
        payload = {"contents": contents}
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self.model}:streamGenerateContent?alt=sse&key={api_key}"
        )
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
            for raw in resp:
                line = raw.decode("utf-8").strip()
                if not line.startswith("data:"):
                    continue
                event = json.loads(line[5:].strip())
                candidates = event.get("candidates") or []
                for candidate in candidates:
                    for part in candidate.get("content", {}).get("parts", []):
                        if part.get("text"):
                            yield part["text"]

    def _read_openai_sse(self, req: urllib.request.Request) -> Generator[str, None, None]:
        with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
            for raw in resp:
                line = raw.decode("utf-8").strip()
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    break
                event = json.loads(data)
                choices = event.get("choices") or []
                if not choices:
                    continue
                delta = choices[0].get("delta") or {}
                if delta.get("content"):
                    yield delta["content"]

    def _send_ingestion_async(self, event: dict[str, Any]) -> None:
        if self.ingestion_sink:
            self.ingestion_sink(event)
            return

        if not self.ingestion_url:
            print(f"ingestion delivery skipped for {event['event_id']}: no ingestion URL", flush=True)
            return

        def send() -> None:
            try:
                req = urllib.request.Request(
                    self.ingestion_url,
                    data=json.dumps(event).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    resp.read()
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                print(f"ingestion delivery failed for {event['event_id']}: {exc}", flush=True)

        threading.Thread(target=send, daemon=True).start()
