package com.crishof.ecommerce.shared.command;

import java.time.LocalDateTime;

/**
 * Orquestador → payment-service. Compensación: reembolsa un cobro previo.
 */
public record RefundPaymentCommand(
        String sagaId,
        String messageId,
        String orderId,
        String reason,
        LocalDateTime issuedAt
) implements SagaCommand {}
