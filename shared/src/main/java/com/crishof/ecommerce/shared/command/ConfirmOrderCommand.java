package com.crishof.ecommerce.shared.command;

import java.time.LocalDateTime;

/**
 * Orquestador → order-service. Confirma el pedido (status PAID).
 */
public record ConfirmOrderCommand(
        String sagaId,
        String messageId,
        String orderId,
        LocalDateTime issuedAt
) implements SagaCommand {}
