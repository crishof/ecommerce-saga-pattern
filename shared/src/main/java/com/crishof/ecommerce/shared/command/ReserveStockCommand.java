package com.crishof.ecommerce.shared.command;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Orquestador → inventory-service. Ordena reservar stock para un pedido.
 */
public record ReserveStockCommand(
        String sagaId,
        String messageId,
        String orderId,
        List<StockReservationRequest> reservations,
        LocalDateTime issuedAt
) implements SagaCommand {

    public record StockReservationRequest(
            String productId,
            int quantity
    ) {}
}
