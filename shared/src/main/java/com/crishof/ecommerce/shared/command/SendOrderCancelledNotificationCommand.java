package com.crishof.ecommerce.shared.command;

import java.time.LocalDateTime;

/**
 * Orquestador → notification-service. Envía la notificación de pedido cancelado.
 */
public record SendOrderCancelledNotificationCommand(
        String sagaId,
        String messageId,
        String orderId,
        long customerId,
        String customerEmail,
        String reason,
        LocalDateTime issuedAt
) implements SagaCommand {}
