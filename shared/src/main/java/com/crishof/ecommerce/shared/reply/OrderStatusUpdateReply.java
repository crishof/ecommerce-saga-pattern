package com.crishof.ecommerce.shared.reply;

import java.time.LocalDateTime;

/**
 * order-service → orquestador. Confirma que el pedido cambió de estado
 * (PAID tras ConfirmOrderCommand, CANCELLED tras CancelOrderCommand).
 */
public record OrderStatusUpdateReply(
        String sagaId,
        String messageId,
        String replyToMessageId,
        String orderId,
        String newStatus,
        ReplyStatus status,
        String failureReason,
        LocalDateTime respondedAt
) implements SagaReply {}
