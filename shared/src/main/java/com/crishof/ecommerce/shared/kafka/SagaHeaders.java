package com.crishof.ecommerce.shared.kafka;

/**
 * Headers Kafka propios de los mensajes de la saga (comandos y réplicas).
 *
 *   x-saga-id        — agrupa todos los mensajes de una misma saga
 *   x-message-id     — identificador único del mensaje (idempotencia)
 *   x-message-type   — nombre simple del comando/réplica (routing en el consumer)
 *   x-correlation-id — trazabilidad end-to-end (compartido con eventos)
 *   x-source-service — servicio emisor
 */
public final class SagaHeaders {

    private SagaHeaders() {}

    public static final String SAGA_ID        = "x-saga-id";
    public static final String MESSAGE_ID     = "x-message-id";
    public static final String MESSAGE_TYPE   = "x-message-type";
    public static final String CORRELATION_ID = "x-correlation-id";
    public static final String SOURCE_SERVICE = "x-source-service";
}
