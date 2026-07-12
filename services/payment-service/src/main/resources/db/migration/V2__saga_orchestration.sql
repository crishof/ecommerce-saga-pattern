-- Adaptación a orquestación: payment-service consume comandos y envía réplicas.

ALTER TABLE outbox_events ADD COLUMN saga_id VARCHAR(64);
ALTER TABLE outbox_events ADD COLUMN message_id VARCHAR(64);

-- El cobro se asocia a la saga que lo originó (evita duplicar operaciones
-- ante retries del mismo comando).
ALTER TABLE payments ADD COLUMN saga_id VARCHAR(64);

CREATE TABLE processed_messages (
    message_id VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);
