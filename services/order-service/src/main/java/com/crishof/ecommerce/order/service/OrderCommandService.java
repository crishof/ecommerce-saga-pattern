package com.crishof.ecommerce.order.service;

import com.crishof.ecommerce.order.domain.Order;
import com.crishof.ecommerce.order.outbox.OutboxEvent;
import com.crishof.ecommerce.order.outbox.OutboxRepository;
import com.crishof.ecommerce.order.repository.OrderRepository;
import com.crishof.ecommerce.shared.command.CancelOrderCommand;
import com.crishof.ecommerce.shared.command.ConfirmOrderCommand;
import com.crishof.ecommerce.shared.event.OrderCancelledEvent;
import com.crishof.ecommerce.shared.event.OrderConfirmedEvent;
import com.crishof.ecommerce.shared.kafka.KafkaTopics;
import com.crishof.ecommerce.shared.reply.OrderStatusUpdateReply;
import com.crishof.ecommerce.shared.reply.ReplyStatus;
import java.time.LocalDateTime;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

/**
 * Aplica los comandos que el orquestador dirige a order-service:
 *  - ConfirmOrderCommand → status PAID   + OrderConfirmedEvent (audit) + réplica
 *  - CancelOrderCommand  → status CANCELLED + OrderCancelledEvent (audit) + réplica
 *
 * Cada método publica una OrderStatusUpdateReply en saga-replies para que el
 * orquestador avance su máquina de estados.
 */
@Service
public class OrderCommandService {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandService.class);

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderCommandService(OrderRepository orderRepository,
                               OutboxRepository outboxRepository,
                               ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    public void confirm(ConfirmOrderCommand command) {
        Order order = load(command.orderId());
        LocalDateTime now = LocalDateTime.now();
        order.setStatus("PAID");
        order.setUpdatedAt(now);
        orderRepository.save(order);

        OrderConfirmedEvent event = new OrderConfirmedEvent(
                order.getId(), order.getCustomerId(), order.getCustomerEmail(),
                order.getTotalAmount(), now, MDC.get("correlationId"));
        saveEventToOutbox(order.getId(), "OrderConfirmed", event);

        OrderStatusUpdateReply reply = new OrderStatusUpdateReply(
                command.sagaId(), UUID.randomUUID().toString(), command.messageId(),
                command.orderId(), "PAID", ReplyStatus.SUCCESS, null, now);
        saveReplyToOutbox(command.sagaId(), reply.messageId(), "OrderStatusUpdateReply", reply);

        log.info("[ORDER] Order {} -> PAID (saga={})", command.orderId(), command.sagaId());
    }

    public void cancel(CancelOrderCommand command) {
        Order order = load(command.orderId());
        LocalDateTime now = LocalDateTime.now();
        order.setStatus("CANCELLED");
        order.setFailureReason(command.reason());
        order.setUpdatedAt(now);
        orderRepository.save(order);

        OrderCancelledEvent event = new OrderCancelledEvent(
                order.getId(), order.getCustomerId(), order.getCustomerEmail(),
                command.reason(), now, MDC.get("correlationId"));
        saveEventToOutbox(order.getId(), "OrderCancelled", event);

        OrderStatusUpdateReply reply = new OrderStatusUpdateReply(
                command.sagaId(), UUID.randomUUID().toString(), command.messageId(),
                command.orderId(), "CANCELLED", ReplyStatus.SUCCESS, null, now);
        saveReplyToOutbox(command.sagaId(), reply.messageId(), "OrderStatusUpdateReply", reply);

        log.info("[ORDER] Order {} -> CANCELLED (saga={}): {}",
                command.orderId(), command.sagaId(), command.reason());
    }

    private Order load(String orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalStateException("Order not found: " + orderId));
    }

    private void saveEventToOutbox(String aggregateId, String eventType, Object payload) {
        OutboxEvent outbox = new OutboxEvent();
        outbox.setAggregateId(aggregateId);
        outbox.setEventType(eventType);
        outbox.setTopic(KafkaTopics.ORDER_EVENTS);
        outbox.setPayloadJson(objectMapper.writeValueAsString(payload));
        outbox.setCorrelationId(MDC.get("correlationId"));
        outbox.setCreatedAt(LocalDateTime.now());
        outboxRepository.save(outbox);
    }

    private void saveReplyToOutbox(String sagaId, String messageId, String replyType, Object reply) {
        OutboxEvent outbox = new OutboxEvent();
        outbox.setAggregateId(sagaId);
        outbox.setEventType(replyType);
        outbox.setTopic(KafkaTopics.SAGA_REPLIES);
        outbox.setPayloadJson(objectMapper.writeValueAsString(reply));
        outbox.setCorrelationId(MDC.get("correlationId"));
        outbox.setSagaId(sagaId);
        outbox.setMessageId(messageId);
        outbox.setCreatedAt(LocalDateTime.now());
        outboxRepository.save(outbox);
    }
}
