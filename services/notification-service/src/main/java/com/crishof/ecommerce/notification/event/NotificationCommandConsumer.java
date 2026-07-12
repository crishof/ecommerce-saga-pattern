package com.crishof.ecommerce.notification.event;

import com.crishof.ecommerce.notification.domain.ProcessedMessage;
import com.crishof.ecommerce.notification.repository.ProcessedMessageRepository;
import com.crishof.ecommerce.notification.service.NotificationService;
import com.crishof.ecommerce.shared.command.SendOrderCancelledNotificationCommand;
import com.crishof.ecommerce.shared.command.SendOrderConfirmedNotificationCommand;
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
 * Consume notification-commands del orquestador y responde con NotificationReply.
 * Idempotencia vía processed_messages.
 */
@Component
public class NotificationCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationCommandConsumer.class);

    private final NotificationService notificationService;
    private final ProcessedMessageRepository processedRepository;
    private final ObjectMapper objectMapper;

    public NotificationCommandConsumer(NotificationService notificationService,
                                       ProcessedMessageRepository processedRepository,
                                       ObjectMapper objectMapper) {
        this.notificationService = notificationService;
        this.processedRepository = processedRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopics.NOTIFICATION_COMMANDS,
            groupId = "notification-service.notification-commands")
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
                log.debug("[NOTIFICATION] Mensaje {} ya procesado, ignorando", messageId);
                return;
            }

            switch (messageType) {
                case "SendOrderConfirmedNotificationCommand" ->
                        notificationService.onOrderConfirmed(
                                objectMapper.readValue(payload, SendOrderConfirmedNotificationCommand.class));
                case "SendOrderCancelledNotificationCommand" ->
                        notificationService.onOrderCancelled(
                                objectMapper.readValue(payload, SendOrderCancelledNotificationCommand.class));
                default -> log.warn("[NOTIFICATION] Comando no soportado: {}", messageType);
            }

            processedRepository.save(new ProcessedMessage(messageId, LocalDateTime.now()));
        } finally {
            MDC.remove("correlationId");
            MDC.remove("sagaId");
        }
    }
}
