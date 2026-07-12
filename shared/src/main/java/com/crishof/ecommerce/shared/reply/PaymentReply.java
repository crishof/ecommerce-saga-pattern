package com.crishof.ecommerce.shared.reply;

import java.time.LocalDateTime;

/**
 * payment-service → orquestador. Resultado de un cobro o reembolso.
 */
public record PaymentReply(
        String sagaId,
        String messageId,
        String replyToMessageId,
        String orderId,
        String paymentId,            // null si FAILURE en el cobro
        String transactionId,        // null si FAILURE
        ReplyStatus status,
        String failureReason,
        LocalDateTime respondedAt
) implements SagaReply {}
