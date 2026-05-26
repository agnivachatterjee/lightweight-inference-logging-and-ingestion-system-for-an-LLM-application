# Architecture Notes

## Components

- `app/server.py`: HTTP server, static UI, chat streaming route, conversation APIs, ingestion route, dashboard API.
- `app/llm.py`: lightweight SDK/wrapper around provider calls.
- `app/ingest.py`: payload validation, PII redaction, metadata extraction.
- `app/db.py`: SQLite schema and persistence helpers.
- `app/static`: browser UI for chat, cancellation, resume, and dashboard views.

## Ingestion Flow

1. The browser opens `/api/chat/stream` with a conversation ID and user message.
2. The server stores the user message and loads the latest 10 messages as short context.
3. `LLMClient.stream_chat` calls the configured provider and yields chunks to the HTTP route.
4. The server writes chunks to the browser as Server-Sent Events.
5. The wrapper finalizes an inference event with timestamps, latency, status, error, token estimates, and redacted previews.
6. The wrapper posts the event to `/api/ingest/inference` in a background thread.
7. Ingestion validates the event, extracts useful dimensions, and stores both the canonical log and query-friendly metadata.

## Logging Strategy

Logging sits at the LLM boundary, not in the UI. This keeps provider instrumentation consistent whether calls come from chat, batch jobs, evals, or future worker processes.

Captured fields include:

- Provider and model
- Conversation/session ID
- Start/end timestamps and latency
- Prompt/completion/total tokens
- Request status and error text
- Redacted input/output previews
- Context count and streaming flag

The current delivery mode is near-real-time best effort. It intentionally does not block the chat response on ingestion success.

## Failure Handling

- Provider failures are streamed back to the UI and logged as `status=error`.
- User cancellation marks the conversation `cancelled` and stops streaming between chunks.
- Browser disconnects mark the active conversation cancelled.
- Ingestion delivery failures are printed by the wrapper. A production version should write failed events to an outbox table and retry.

## Scaling Considerations

For higher traffic:

- Move from SQLite to Postgres.
- On Vercel, use managed Postgres/Neon/Supabase/Vercel Postgres instead of the default `/tmp` SQLite demo database.
- Add a queue or log stream between SDKs and ingestion.
- Split chat and ingestion into independent services.
- Use an outbox pattern for guaranteed event delivery.
- Add background rollups for dashboard metrics.
- Partition inference logs by time if volume grows quickly.
- Add stronger auth, tenant IDs, retention controls, and DLP-grade redaction.

## Event-Based Extension

The ingestion contract is already event-shaped: each inference is a self-contained immutable event with an ID, timestamps, payload, and extracted metadata. Replacing the direct HTTP post with Kafka, NATS, Redis Streams, or SQS would not require changing the chat UI or provider adapters.
