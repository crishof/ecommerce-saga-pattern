package com.crishof.ecommerce.order.event;

import com.crishof.ecommerce.order.domain.ProcessedMessage;
import com.crishof.ecommerce.order.repository.ProcessedMessageRepository;
import com.crishof.ecommerce.order.service.OrderCommandService;
import com.crishof.ecommerce.shared.command.CancelOrderCommand;
import com.crishof.ecommerce.shared.command.ConfirmOrderCommand;
import com.crishof.ecommerce.shared.kafka.KafkaTopics;
import com.crishof.ecommerce.shared.kafka.SagaHeaders;
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
 * Consume order-commands del orquestador. Reemplaza al OrderEventConsumer del
 * proyecto 19: order-service ya no reacciona a eventos de otros servicios, solo
 * obedece los comandos del coordinador y le responde con réplicas.
 *
 * Idempotencia: cada messageId se registra en processed_messages; un comando
 * duplicado (retry de Kafka) se ignora.
 */
@Component
public class OrderCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandConsumer.class);

    private final OrderCommandService orderCommandService;
    private final ProcessedMessageRepository processedRepository;
    private final ObjectMapper objectMapper;

    public OrderCommandConsumer(OrderCommandService orderCommandService,
                                ProcessedMessageRepository processedRepository,
                                ObjectMapper objectMapper) {
        this.orderCommandService = orderCommandService;
        this.processedRepository = processedRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopics.ORDER_COMMANDS, groupId = "order-service.order-commands")
    @Transactional
    public void onCommand(
            @Payload String payload,
            @Header(SagaHeaders.MESSAGE_TYPE) String messageType,
            @Header(SagaHeaders.MESSAGE_ID) String messageId,
            @Header(SagaHeaders.SAGA_ID) String sagaId,
            @Header(name = SagaHeaders.CORRELATION_ID, required = false) String correlationId) {

        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }
        MDC.put("sagaId", sagaId);
        try {
            if (processedRepository.existsById(messageId)) {
                log.debug("[ORDER] Mensaje {} ya procesado, ignorando", messageId);
                return;
            }

            switch (messageType) {
                case "ConfirmOrderCommand" ->
                        orderCommandService.confirm(objectMapper.readValue(payload, ConfirmOrderCommand.class));
                case "CancelOrderCommand" ->
                        orderCommandService.cancel(objectMapper.readValue(payload, CancelOrderCommand.class));
                default -> log.warn("[ORDER] Comando no soportado: {}", messageType);
            }

            processedRepository.save(new ProcessedMessage(messageId, LocalDateTime.now()));
        } finally {
            MDC.remove("correlationId");
            MDC.remove("sagaId");
        }
    }
}
