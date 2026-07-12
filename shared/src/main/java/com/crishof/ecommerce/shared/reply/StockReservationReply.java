package com.crishof.ecommerce.shared.reply;

import java.time.LocalDateTime;

/**
 * inventory-service → orquestador. Resultado de reservar o liberar stock.
 */
public record StockReservationReply(
        String sagaId,
        String messageId,
        String replyToMessageId,
        String orderId,
        ReplyStatus status,
        String failureReason,       // null si SUCCESS
        LocalDateTime respondedAt
) implements SagaReply {}
