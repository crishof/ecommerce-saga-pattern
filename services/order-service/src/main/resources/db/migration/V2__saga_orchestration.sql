-- Adaptación a orquestación: order-service consume comandos y envía réplicas.

-- El outbox ahora transporta también mensajes de saga (comandos/réplicas),
-- por lo que necesita saga_id y message_id para poblar los headers Kafka.
ALTER TABLE outbox_events ADD COLUMN saga_id VARCHAR(64);
ALTER TABLE outbox_events ADD COLUMN message_id VARCHAR(64);

-- Idempotencia: IDs de comandos ya procesados.
CREATE TABLE processed_messages (
    message_id VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);
