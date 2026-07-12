package com.crishof.ecommerce.shared.reply;

import java.time.LocalDateTime;

/**
 * notification-service → orquestador. Confirma el envío de una notificación.
 */
public record NotificationReply(
        String sagaId,
        String messageId,
        String replyToMessageId,
        ReplyStatus status,
        String failureReason,
        LocalDateTime respondedAt
) implements SagaReply {}
