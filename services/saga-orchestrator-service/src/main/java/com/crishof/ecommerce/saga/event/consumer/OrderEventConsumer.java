package com.crishof.ecommerce.saga.event.consumer;

import com.crishof.ecommerce.saga.domain.ProcessedMessage;
import com.crishof.ecommerce.saga.repository.ProcessedMessageRepository;
import com.crishof.ecommerce.saga.statemachine.SagaStateMachine;
import com.crishof.ecommerce.shared.event.OrderPlacedEvent;
import com.crishof.ecommerce.shared.kafka.KafkaTopics;
import java.time.LocalDateTime;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * Consume order-events y dispara una saga nueva al ver un OrderPlacedEvent.
 *
 * Los eventos OrderConfirmed/OrderCancelled (audit trail) los generamos NOSOTROS
 * a través de order-service, así que aquí se ignoran para no reprocesarlos.
 */
@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final SagaStateMachine stateMachine;
    private final ProcessedMessageRepository processedRepository;
    private final ObjectMapper objectMapper;

    public OrderEventConsumer(SagaStateMachine stateMachine,
                              ProcessedMessageRepository processedRepository,
                              ObjectMapper objectMapper) {
        this.stateMachine = stateMachine;
        this.processedRepository = processedRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopics.ORDER_EVENTS, groupId = "saga-orchestrator.order-events")
    @Transactional
    public void onEvent(
            @Payload String payload,
            @Header(name = KafkaTopics.HEADER_EVENT_TYPE, required = false) String eventType,
            @Header(name = KafkaTopics.HEADER_CORRELATION_ID, required = false) String correlationId,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {

        // Solo OrderPlaced dispara sagas; el resto (audit) se ignora.
        if (!"OrderPlaced".equals(eventType)) {
            return;
        }

        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }
        try {
            // messageId sintético a partir de la key (orderId) para idempotencia.
            String messageId = "orderplaced-" + (key != null ? key : UUID.randomUUID().toString());
            if (processedRepository.existsById(messageId)) {
                log.debug("[SAGA] OrderPlaced {} ya procesado, ignorando", messageId);
                return;
            }

            OrderPlacedEvent event = objectMapper.readValue(payload, OrderPlacedEvent.class);
            stateMachine.startSaga(event, correlationId);

            processedRepository.save(new ProcessedMessage(messageId, LocalDateTime.now()));
        } finally {
            MDC.remove("correlationId");
        }
    }
}
