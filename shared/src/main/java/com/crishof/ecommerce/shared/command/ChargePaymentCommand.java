package com.crishof.ecommerce.shared.command;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Orquestador → payment-service. Ordena cobrar el importe del pedido.
 *
 * A diferencia del proyecto 19 (choreography), el importe viaja DENTRO del
 * comando: payment-service ya no necesita mantener una proyección local del
 * pedido para conocer el monto.
 */
public record ChargePaymentCommand(
        String sagaId,
        String messageId,
        String orderId,
        long customerId,
        BigDecimal amount,
        LocalDateTime issuedAt
) implements SagaCommand {}
