package com.crishof.ecommerce.inventory.service;

import com.crishof.ecommerce.inventory.domain.StockReservation;
import com.crishof.ecommerce.inventory.outbox.OutboxEvent;
import com.crishof.ecommerce.inventory.outbox.OutboxRepository;
import com.crishof.ecommerce.inventory.repository.StockReservationRepository;
import com.crishof.ecommerce.shared.command.ReleaseStockCommand;
import com.crishof.ecommerce.shared.command.ReserveStockCommand;
import com.crishof.ecommerce.shared.kafka.KafkaTopics;
import com.crishof.ecommerce.shared.reply.ReplyStatus;
import com.crishof.ecommerce.shared.reply.StockReservationReply;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

/**
 * Reserva y libera stock, dirigido por comandos del orquestador.
 *
 * COMPROMISO DE DISEÑO (académico): inventory-service no conoce el stock real
 * canónico (vive en catalog-service); solo mantiene RESERVAS. La reserva SIEMPRE
 * tiene éxito (se asume stock suficiente); el fallo de la saga se demuestra vía
 * payment-service. La compensación se ejerce liberando reservas (ReleaseStock).
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final StockReservationRepository reservationRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public InventoryService(StockReservationRepository reservationRepository,
                            OutboxRepository outboxRepository,
                            ObjectMapper objectMapper) {
        this.reservationRepository = reservationRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    /** Reserva stock para el pedido. Publica StockReservationReply(SUCCESS). */
    public void reserve(ReserveStockCommand command) {
        String orderId = command.orderId();
        LocalDateTime now = LocalDateTime.now();

        // Idempotencia adicional por orderId: si ya hay reservas, no las duplicamos.
        if (!reservationRepository.existsByOrderId(orderId)) {
            for (ReserveStockCommand.StockReservationRequest req : command.reservations()) {
                StockReservation reservation = new StockReservation();
                reservation.setOrderId(orderId);
                reservation.setSagaId(command.sagaId());
                reservation.setProductId(req.productId());
                reservation.setQuantity(req.quantity());
                reservation.setStatus("HELD");
                reservation.setCreatedAt(now);
                reservationRepository.save(reservation);
            }
            log.info("[INVENTORY] Stock reservado para order {} ({} líneas, saga={})",
                    orderId, command.reservations().size(), command.sagaId());
        } else {
            log.info("[INVENTORY] Reservas para order {} ya existen, no se duplican", orderId);
        }

        StockReservationReply reply = new StockReservationReply(
                command.sagaId(), UUID.randomUUID().toString(), command.messageId(),
                orderId, ReplyStatus.SUCCESS, null, now);
        saveReplyToOutbox(command.sagaId(), reply.messageId(), reply);
    }

    /** Libera las reservas del pedido (compensación). Publica StockReservationReply(SUCCESS). */
    public void release(ReleaseStockCommand command) {
        String orderId = command.orderId();
        LocalDateTime now = LocalDateTime.now();
        List<StockReservation> reservations = reservationRepository.findByOrderId(orderId);
        for (StockReservation reservation : reservations) {
            if (!"RELEASED".equals(reservation.getStatus())) {
                reservation.setStatus("RELEASED");
                reservation.setReleasedAt(now);
                reservationRepository.save(reservation);
            }
        }
        log.info("[INVENTORY] Reservas liberadas para order {} ({} líneas, saga={})",
                orderId, reservations.size(), command.sagaId());

        StockReservationReply reply = new StockReservationReply(
                command.sagaId(), UUID.randomUUID().toString(), command.messageId(),
                orderId, ReplyStatus.SUCCESS, null, now);
        saveReplyToOutbox(command.sagaId(), reply.messageId(), reply);
    }

    private void saveReplyToOutbox(String sagaId, String messageId, Object reply) {
        OutboxEvent outbox = new OutboxEvent();
        outbox.setAggregateId(sagaId);
        outbox.setEventType("StockReservationReply");
        outbox.setTopic(KafkaTopics.SAGA_REPLIES);
        outbox.setPayloadJson(objectMapper.writeValueAsString(reply));
        outbox.setCorrelationId(MDC.get("correlationId"));
        outbox.setSagaId(sagaId);
        outbox.setMessageId(messageId);
        outbox.setCreatedAt(LocalDateTime.now());
        outboxRepository.save(outbox);
    }
}
