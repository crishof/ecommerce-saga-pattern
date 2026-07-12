package com.crishof.ecommerce.shared.command;

import java.time.LocalDateTime;

/**
 * Orquestador → inventory-service. Compensación: libera el stock reservado.
 */
public record ReleaseStockCommand(
        String sagaId,
        String messageId,
        String orderId,
        String reason,
        LocalDateTime issuedAt
) implements SagaCommand {}
