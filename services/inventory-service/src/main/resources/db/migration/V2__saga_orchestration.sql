-- Adaptación a orquestación: inventory-service consume comandos y envía réplicas.

ALTER TABLE outbox_events ADD COLUMN saga_id VARCHAR(64);
ALTER TABLE outbox_events ADD COLUMN message_id VARCHAR(64);

-- Las reservas se asocian a su saga (trazabilidad + idempotencia por sagaId).
ALTER TABLE stock_reservations ADD COLUMN saga_id VARCHAR(64);

CREATE TABLE processed_messages (
    message_id VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);
