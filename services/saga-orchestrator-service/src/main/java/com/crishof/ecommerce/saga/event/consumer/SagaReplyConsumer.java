package com.crishof.ecommerce.saga.event.consumer;

import com.crishof.ecommerce.saga.domain.ProcessedMessage;
import com.crishof.ecommerce.saga.repository.ProcessedMessageRepository;
import com.crishof.ecommerce.saga.statemachine.SagaStateMachine;
import com.crishof.ecommerce.shared.kafka.KafkaTopics;
import com.crishof.ecommerce.shared.kafka.SagaHeaders;
import com.crishof.ecommerce.shared.reply.NotificationReply;
import com.crishof.ecommerce.shared.reply.OrderStatusUpdateReply;
import com.crishof.ecommerce.shared.reply.PaymentReply;
import com.crishof.ecommerce.shared.reply.StockReservationReply;
import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * Consume saga-replies y avanza la máquina de estados. Es el único consumidor de
 * este topic. Idempotencia vía processed_messages sobre el messageId de la réplica.
 */
@Component
public class SagaReplyConsumer {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyConsumer.class);

    private final SagaStateMachine stateMachine;
    private final ProcessedMessageRepository processedRepository;
    private final ObjectMapper objectMapper;

    public SagaReplyConsumer(SagaStateMachine stateMachine,
                             ProcessedMessageRepository processedRepository,
                             ObjectMapper objectMapper) {
        this.stateMachine = stateMachine;
        this.processedRepository = processedRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopics.SAGA_REPLIES, groupId = "saga-orchestrator.saga-replies")
    @Transactional
    public void onReply(
            @Payload String payload,
            @Header(SagaHeaders.MESSAGE_TYPE) String messageType,
            @Header(SagaHeaders.MESSAGE_ID) String messageId,
            @Header(name = SagaHeaders.SAGA_ID, required = false) String sagaId,
            @Header(name = SagaHeaders.CORRELATION_ID, required = false) String correlationId) {

        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }
        if (sagaId != null) {
            MDC.put("sagaId", sagaId);
        }
        try {
            if (processedRepository.existsById(messageId)) {
                log.debug("[SAGA] Réplica {} ya procesada, ignorando", messageId);
                return;
            }

            switch (messageType) {
                case "StockReservationReply" -> stateMachine.handleStockReservationReply(
                        objectMapper.readValue(payload, StockReservationReply.class));
                case "PaymentReply" -> stateMachine.handlePaymentReply(
                        objectMapper.readValue(payload, PaymentReply.class));
                case "OrderStatusUpdateReply" -> stateMachine.handleOrderStatusUpdateReply(
                        objectMapper.readValue(payload, OrderStatusUpdateReply.class));
                case "NotificationReply" -> stateMachine.handleNotificationReply(
                        objectMapper.readValue(payload, NotificationReply.class));
                default -> log.warn("[SAGA] Réplica no soportada: {}", messageType);
            }

            processedRepository.save(new ProcessedMessage(messageId, LocalDateTime.now()));
        } finally {
            MDC.remove("correlationId");
            MDC.remove("sagaId");
        }
    }
}
