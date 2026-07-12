package com.crishof.ecommerce.shared.command;

import java.time.LocalDateTime;

/**
 * Orquestador → order-service. Compensación: cancela el pedido (status CANCELLED).
 */
public record CancelOrderCommand(
        String sagaId,
        String messageId,
        String orderId,
        String reason,
        LocalDateTime issuedAt
) implements SagaCommand {}
