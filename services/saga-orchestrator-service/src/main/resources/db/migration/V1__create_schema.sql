-- Instancias de saga: una fila cuenta TODA la historia de una saga (el diferencial
-- clave frente al proyecto 19, donde el estado estaba disperso en cada servicio).
CREATE TABLE saga_instances (
    saga_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    customer_id BIGINT NOT NULL,
    customer_email VARCHAR(255) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    lines_json JSONB NOT NULL,
    state VARCHAR(30) NOT NULL,
    failure_reason VARCHAR(500),
    correlation_id VARCHAR(64),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP
);
CREATE INDEX idx_sagas_state ON saga_instances(state);
CREATE INDEX idx_sagas_expires ON saga_instances(expires_at)
    WHERE completed_at IS NULL;
CREATE INDEX idx_sagas_order ON saga_instances(order_id);

-- Histórico de pasos: cada comando enviado y su réplica.
CREATE TABLE saga_steps (
    id BIGSERIAL PRIMARY KEY,
    saga_id VARCHAR(64) NOT NULL REFERENCES saga_instances(saga_id),
    step_number INTEGER NOT NULL,
    step_type VARCHAR(50) NOT NULL,       -- ej: RESERVE_STOCK, CHARGE_PAYMENT
    message_id VARCHAR(64) NOT NULL,      -- messageId del comando enviado
    command_type VARCHAR(80) NOT NULL,
    command_json JSONB NOT NULL,
    reply_status VARCHAR(20),             -- SUCCESS, FAILURE (null hasta recibir)
    reply_json JSONB,
    dispatched_at TIMESTAMP NOT NULL,
    replied_at TIMESTAMP
);
CREATE INDEX idx_steps_saga ON saga_steps(saga_id, step_number);
CREATE UNIQUE INDEX idx_steps_message ON saga_steps(message_id);

-- Idempotencia de eventos y réplicas entrantes.
CREATE TABLE processed_messages (
    message_id VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Outbox para publicar comandos (y así el orquestador sobrevive a caídas).
CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    topic VARCHAR(100) NOT NULL,
    payload_json JSONB NOT NULL,
    saga_id VARCHAR(64),                  -- para trazabilidad
    message_id VARCHAR(64) NOT NULL,      -- messageId del comando
    correlation_id VARCHAR(64),
    created_at TIMESTAMP NOT NULL,
    published_at TIMESTAMP
);
CREATE INDEX idx_outbox_unpublished ON outbox_events(published_at, created_at)
    WHERE published_at IS NULL;
