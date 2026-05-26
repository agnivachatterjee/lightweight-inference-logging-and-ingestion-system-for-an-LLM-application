from __future__ import annotations

import json
import os
import signal
import threading
import uuid
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from app.db import Database, from_env
from app.ingest import extract_metadata, validate_inference_event
from app.llm import LLMClient, ProviderError


ROOT = Path(__file__).resolve().parent
STATIC_ROOT = ROOT / "static"


def load_dotenv() -> None:
    if os.getenv("VERCEL"):
        return
    env_path = ROOT.parent / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_dotenv()
DB: Database = from_env()
CANCELLED: set[str] = set()
CANCEL_LOCK = threading.Lock()


def env_provider() -> tuple[str, str]:
    provider = os.getenv("LLM_PROVIDER", "mock").strip() or "mock"
    default_models = {
        "mock": "mock-small",
        "openai": "gpt-4.1-mini",
        "anthropic": "claude-3-5-sonnet-latest",
        "gemini": "gemini-2.5-flash",
    }
    model = os.getenv("LLM_MODEL", default_models.get(provider, "mock-small")).strip()
    return provider, model


class Handler(SimpleHTTPRequestHandler):
    server_version = "InferenceLogger/0.1"

    def translate_path(self, path: str) -> str:
        parsed = urlparse(path)
        clean = parsed.path.lstrip("/") or "index.html"
        if clean.startswith("api/"):
            return str(STATIC_ROOT / "index.html")
        return str(STATIC_ROOT / clean)

    def log_message(self, format: str, *args: Any) -> None:
        print(f"{self.address_string()} - {format % args}", flush=True)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/health":
            self.json_response({"ok": True})
        elif parsed.path == "/api/conversations":
            self.json_response({"conversations": DB.list_conversations()})
        elif parsed.path.startswith("/api/conversations/"):
            conversation_id = parsed.path.split("/")[-1]
            conversation = DB.get_conversation(conversation_id)
            if not conversation:
                self.error_json(HTTPStatus.NOT_FOUND, "conversation not found")
                return
            self.json_response({"conversation": conversation, "messages": DB.get_messages(conversation_id)})
        elif parsed.path == "/api/dashboard":
            self.json_response(DB.dashboard())
        elif parsed.path == "/api/chat/stream":
            self.handle_chat_stream(parsed.query)
        else:
            super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/ingest/inference":
            self.handle_ingest()
        elif parsed.path == "/api/conversations":
            self.handle_create_conversation()
        elif parsed.path.endswith("/cancel") and parsed.path.startswith("/api/conversations/"):
            conversation_id = parsed.path.split("/")[-2]
            self.handle_cancel(conversation_id)
        else:
            self.error_json(HTTPStatus.NOT_FOUND, "route not found")

    def read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        return json.loads(raw or "{}")

    def json_response(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def error_json(self, status: HTTPStatus, message: str) -> None:
        self.json_response({"error": message}, status)

    def handle_create_conversation(self) -> None:
        body = self.read_json()
        provider, model = env_provider()
        conversation_id = body.get("id") or str(uuid.uuid4())
        title = (body.get("title") or "New conversation").strip()[:80]
        DB.create_conversation(conversation_id, title, provider, model)
        self.json_response({"conversation": DB.get_conversation(conversation_id)}, HTTPStatus.CREATED)

    def handle_cancel(self, conversation_id: str) -> None:
        with CANCEL_LOCK:
            CANCELLED.add(conversation_id)
        DB.cancel_conversation(conversation_id)
        self.json_response({"ok": True, "conversation_id": conversation_id})

    def handle_ingest(self) -> None:
        try:
            event = validate_inference_event(self.read_json())
            extracted = extract_metadata(event)
            DB.store_inference_log(event, extracted)
            self.json_response({"ok": True, "log_id": event["event_id"]}, HTTPStatus.ACCEPTED)
        except (json.JSONDecodeError, ValueError) as exc:
            self.error_json(HTTPStatus.BAD_REQUEST, str(exc))

    def handle_chat_stream(self, query: str) -> None:
        params = parse_qs(query)
        message = (params.get("message") or [""])[0].strip()
        conversation_id = (params.get("conversation_id") or [""])[0].strip() or str(uuid.uuid4())
        if not message:
            self.error_json(HTTPStatus.BAD_REQUEST, "message is required")
            return

        provider, model = env_provider()
        existing = DB.get_conversation(conversation_id)
        if not existing:
            DB.create_conversation(conversation_id, message[:60] or "New conversation", provider, model)
        elif existing["status"] == "cancelled":
            with CANCEL_LOCK:
                CANCELLED.discard(conversation_id)
            DB.activate_conversation(conversation_id)

        DB.add_message(str(uuid.uuid4()), conversation_id, "user", message)
        context = [{"role": "system", "content": "You are a concise, helpful assistant."}]
        context.extend({"role": row["role"], "content": row["content"]} for row in DB.get_messages(conversation_id, limit=10))

        base_url = "" if os.getenv("VERCEL") else f"http://127.0.0.1:{self.server.server_port}"
        client = LLMClient(
            provider,
            model,
            f"{base_url}/api/ingest/inference",
            ingestion_sink=store_ingestion_event if os.getenv("VERCEL") else None,
        )

        if os.getenv("VERCEL"):
            self.handle_chat_buffered(conversation_id, provider, model, client, context)
            return

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        assistant_parts: list[str] = []

        def is_cancelled() -> bool:
            with CANCEL_LOCK:
                return conversation_id in CANCELLED

        try:
            self.sse("meta", {"conversation_id": conversation_id, "provider": provider, "model": model})
            result = client.stream_chat(conversation_id, context, cancel_check=is_cancelled)
            for chunk in result.chunks:
                assistant_parts.append(chunk)
                self.sse("token", {"text": chunk})
            status = "cancelled" if is_cancelled() else "done"
            assistant_text = "".join(assistant_parts).strip()
            if assistant_text:
                DB.add_message(str(uuid.uuid4()), conversation_id, "assistant", assistant_text, {"status": status})
            self.sse(status, {"conversation_id": conversation_id})
        except ProviderError as exc:
            self.sse("failure", {"error": str(exc)})
        except BrokenPipeError:
            with CANCEL_LOCK:
                CANCELLED.add(conversation_id)
            DB.cancel_conversation(conversation_id)
        except Exception as exc:
            self.sse("failure", {"error": str(exc)})
        finally:
            try:
                self.wfile.flush()
            except OSError:
                pass

    def sse(self, event: str, payload: dict[str, Any]) -> None:
        data = f"event: {event}\ndata: {json.dumps(payload)}\n\n".encode("utf-8")
        self.wfile.write(data)
        self.wfile.flush()

    def handle_chat_buffered(
        self,
        conversation_id: str,
        provider: str,
        model: str,
        client: LLMClient,
        context: list[dict[str, str]],
    ) -> None:
        events: list[bytes] = []
        assistant_parts: list[str] = []

        def add_event(name: str, payload: dict[str, Any]) -> None:
            events.append(f"event: {name}\ndata: {json.dumps(payload)}\n\n".encode("utf-8"))

        add_event("meta", {"conversation_id": conversation_id, "provider": provider, "model": model})
        try:
            result = client.stream_chat(conversation_id, context, cancel_check=lambda: False)
            for chunk in result.chunks:
                assistant_parts.append(chunk)
                add_event("token", {"text": chunk})
            assistant_text = "".join(assistant_parts).strip()
            if assistant_text:
                DB.add_message(str(uuid.uuid4()), conversation_id, "assistant", assistant_text, {"status": "done"})
            add_event("done", {"conversation_id": conversation_id})
        except ProviderError as exc:
            add_event("failure", {"error": str(exc)})
        except Exception as exc:
            add_event("failure", {"error": str(exc)})

        body = b"".join(events)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.close_connection = True


def store_ingestion_event(event: dict[str, Any]) -> None:
    normalized = validate_inference_event(event)
    extracted = extract_metadata(normalized)
    DB.store_inference_log(normalized, extracted)


def main() -> None:
    port = int(os.getenv("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"serving on http://localhost:{port}", flush=True)

    def stop(_signum: int, _frame: Any) -> None:
        server.shutdown()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    server.serve_forever()


if __name__ == "__main__":
    main()
