package com.crishof.ecommerce.notification.service;

import com.crishof.ecommerce.notification.domain.Notification;
import com.crishof.ecommerce.notification.outbox.OutboxEvent;
import com.crishof.ecommerce.notification.outbox.OutboxRepository;
import com.crishof.ecommerce.notification.repository.NotificationRepository;
import com.crishof.ecommerce.shared.command.SendOrderCancelledNotificationCommand;
import com.crishof.ecommerce.shared.command.SendOrderConfirmedNotificationCommand;
import com.crishof.ecommerce.shared.kafka.KafkaTopics;
import com.crishof.ecommerce.shared.reply.NotificationReply;
import com.crishof.ecommerce.shared.reply.ReplyStatus;
import java.time.LocalDateTime;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

/**
 * Registra notificaciones (simuladas) dirigido por comandos del orquestador y
 * responde con NotificationReply para cerrar la saga.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final NotificationRepository notificationRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public NotificationService(NotificationRepository notificationRepository,
                               OutboxRepository outboxRepository,
                               ObjectMapper objectMapper) {
        this.notificationRepository = notificationRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    public void onOrderConfirmed(SendOrderConfirmedNotificationCommand command) {
        Notification notification = new Notification();
        notification.setOrderId(command.orderId());
        notification.setCustomerId(command.customerId());
        notification.setCustomerEmail(command.customerEmail());
        notification.setType("ORDER_CONFIRMED");
        notification.setSubject("Tu pedido " + command.orderId() + " está confirmado");
        notification.setBody("Hemos cobrado " + command.totalAmount()
                + " y tu pedido está en preparación. ¡Gracias por tu compra!");
        notification.setSentAt(LocalDateTime.now());
        notificationRepository.save(notification);
        log.info("[NOTIFICATION] Confirmación enviada a {} (order {}, saga={})",
                command.customerEmail(), command.orderId(), command.sagaId());

        reply(command.sagaId(), command.messageId());
    }

    public void onOrderCancelled(SendOrderCancelledNotificationCommand command) {
        Notification notification = new Notification();
        notification.setOrderId(command.orderId());
        notification.setCustomerId(command.customerId());
        notification.setCustomerEmail(command.customerEmail());
        notification.setType("ORDER_CANCELLED");
        notification.setSubject("Tu pedido " + command.orderId() + " ha sido cancelado");
        notification.setBody("Lo sentimos, tu pedido no pudo completarse. Motivo: " + command.reason());
        notification.setSentAt(LocalDateTime.now());
        notificationRepository.save(notification);
        log.warn("[NOTIFICATION] Cancelación enviada a {} (order {}, saga={}): {}",
                command.customerEmail(), command.orderId(), command.sagaId(), command.reason());

        reply(command.sagaId(), command.messageId());
    }

    private void reply(String sagaId, String replyToMessageId) {
        String messageId = UUID.randomUUID().toString();
        NotificationReply reply = new NotificationReply(
                sagaId, messageId, replyToMessageId,
                ReplyStatus.SUCCESS, null, LocalDateTime.now());

        OutboxEvent outbox = new OutboxEvent();
        outbox.setAggregateId(sagaId);
        outbox.setEventType("NotificationReply");
        outbox.setTopic(KafkaTopics.SAGA_REPLIES);
        outbox.setPayloadJson(objectMapper.writeValueAsString(reply));
        outbox.setCorrelationId(MDC.get("correlationId"));
        outbox.setSagaId(sagaId);
        outbox.setMessageId(messageId);
        outbox.setCreatedAt(LocalDateTime.now());
        outboxRepository.save(outbox);
    }
}
