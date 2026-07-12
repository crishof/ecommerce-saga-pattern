package com.crishof.ecommerce.shared.command;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Orquestador → notification-service. Envía la notificación de pedido confirmado.
 */
public record SendOrderConfirmedNotificationCommand(
        String sagaId,
        String messageId,
        String orderId,
        long customerId,
        String customerEmail,
        BigDecimal totalAmount,
        LocalDateTime issuedAt
) implements SagaCommand {}
