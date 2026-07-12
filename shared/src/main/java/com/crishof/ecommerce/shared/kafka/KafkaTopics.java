package com.crishof.ecommerce.shared.kafka;

/**
 * Nombres de topics de Kafka del stack de orquestación.
 *
 * Tres estilos de mensaje:
 *   EVENTO   → order-events (audit trail; OrderPlaced dispara la saga)
 *   COMANDO  → *-commands (orquestador → servicio destino)
 *   RÉPLICA  → saga-replies (servicios → orquestador, único consumidor)
 *
 * Headers estándar de eventos "informativos" (order-events):
 *   x-event-type      — nombre corto del tipo (ej "OrderPlaced")
 *   x-source-service  — servicio emisor
 *   x-correlation-id  — trazabilidad end-to-end
 *
 * Los headers específicos de la saga viven en {@link SagaHeaders}.
 */
public final class KafkaTopics {

    private KafkaTopics() {}

    // ═══ Eventos informativos (audit trail) ═══
    public static final String ORDER_EVENTS = "order-events";

    // ═══ Comandos (orquestador → servicios) ═══
    public static final String INVENTORY_COMMANDS    = "inventory-commands";
    public static final String PAYMENT_COMMANDS      = "payment-commands";
    public static final String ORDER_COMMANDS        = "order-commands";
    public static final String NOTIFICATION_COMMANDS = "notification-commands";

    // ═══ Réplicas (servicios → orquestador) ═══
    public static final String SAGA_REPLIES = "saga-replies";

    // ═══ Headers de eventos informativos ═══
    public static final String HEADER_EVENT_TYPE     = "x-event-type";
    public static final String HEADER_SOURCE_SERVICE = "x-source-service";
    public static final String HEADER_CORRELATION_ID = "x-correlation-id";
}
