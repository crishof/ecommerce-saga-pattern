-- Adaptación a orquestación: notification-service ahora consume comandos y, por
-- primera vez, EMITE réplicas → necesita su propio outbox + idempotencia.

CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    topic VARCHAR(100) NOT NULL,
    payload_json JSONB NOT NULL,
    correlation_id VARCHAR(64),
    saga_id VARCHAR(64),
    message_id VARCHAR(64),
    created_at TIMESTAMP NOT NULL,
    published_at TIMESTAMP
);
CREATE INDEX idx_outbox_unpublished ON outbox_events(published_at, created_at)
    WHERE published_at IS NULL;

CREATE TABLE processed_messages (
    message_id VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);
