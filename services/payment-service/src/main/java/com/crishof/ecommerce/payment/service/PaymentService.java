package com.crishof.ecommerce.payment.service;

import com.crishof.ecommerce.payment.config.PaymentsProperties;
import com.crishof.ecommerce.payment.domain.Payment;
import com.crishof.ecommerce.payment.outbox.OutboxEvent;
import com.crishof.ecommerce.payment.outbox.OutboxRepository;
import com.crishof.ecommerce.payment.repository.PaymentRepository;
import com.crishof.ecommerce.shared.command.ChargePaymentCommand;
import com.crishof.ecommerce.shared.command.RefundPaymentCommand;
import com.crishof.ecommerce.shared.kafka.KafkaTopics;
import com.crishof.ecommerce.shared.reply.PaymentReply;
import com.crishof.ecommerce.shared.reply.ReplyStatus;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

/**
 * Gateway de pagos simulado, dirigido por comandos del orquestador.
 *
 * A diferencia del proyecto 19, el importe llega DENTRO del ChargePaymentCommand,
 * por lo que ya NO se mantiene una proyección local del pedido (registerPending
 * desaparece). Esto elimina la dependencia de orden entre OrderPlaced y el cobro.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final PaymentsProperties properties;

    public PaymentService(PaymentRepository paymentRepository,
                          OutboxRepository outboxRepository,
                          ObjectMapper objectMapper,
                          PaymentsProperties properties) {
        this.paymentRepository = paymentRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    /** Intenta cobrar. Publica PaymentReply(SUCCESS|FAILURE). */
    public void charge(ChargePaymentCommand command) {
        String orderId = command.orderId();
        simulateLatency();

        String paymentId = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();
        boolean success = ThreadLocalRandom.current().nextDouble() >= properties.failureRate();

        Payment payment = new Payment();
        payment.setId(paymentId);
        payment.setOrderId(orderId);
        payment.setSagaId(command.sagaId());
        payment.setCustomerId(command.customerId());
        payment.setAmount(command.amount());
        payment.setProcessedAt(now);

        PaymentReply reply;
        if (success) {
            String transactionId = "TX-" + UUID.randomUUID().toString().substring(0, 12).toUpperCase();
            payment.setStatus("SUCCESS");
            payment.setTransactionId(transactionId);
            paymentRepository.save(payment);

            reply = new PaymentReply(command.sagaId(), UUID.randomUUID().toString(), command.messageId(),
                    orderId, paymentId, transactionId, ReplyStatus.SUCCESS, null, now);
            log.info("[PAYMENT] Cobro OK order {} tx {} (saga={})", orderId, transactionId, command.sagaId());
        } else {
            String reason = "Fondos insuficientes / rechazo del emisor";
            payment.setStatus("FAILED");
            payment.setFailureReason(reason);
            paymentRepository.save(payment);

            reply = new PaymentReply(command.sagaId(), UUID.randomUUID().toString(), command.messageId(),
                    orderId, paymentId, null, ReplyStatus.FAILURE, reason, now);
            log.warn("[PAYMENT] Cobro RECHAZADO order {} (saga={}): {}", orderId, command.sagaId(), reason);
        }
        saveReplyToOutbox(command.sagaId(), reply.messageId(), reply);
    }

    /** Reembolsa un cobro previo (compensación). Siempre responde SUCCESS. */
    public void refund(RefundPaymentCommand command) {
        LocalDateTime now = LocalDateTime.now();
        Payment payment = paymentRepository.findByOrderId(command.orderId()).orElse(null);
        if (payment != null && "SUCCESS".equals(payment.getStatus())) {
            payment.setStatus("REFUNDED");
            payment.setFailureReason("Refund: " + command.reason());
            payment.setProcessedAt(now);
            paymentRepository.save(payment);
            log.info("[PAYMENT] Reembolso aplicado order {} (saga={})", command.orderId(), command.sagaId());
        } else {
            log.info("[PAYMENT] Nada que reembolsar para order {} (saga={})",
                    command.orderId(), command.sagaId());
        }

        PaymentReply reply = new PaymentReply(command.sagaId(), UUID.randomUUID().toString(),
                command.messageId(), command.orderId(),
                payment != null ? payment.getId() : null, null,
                ReplyStatus.SUCCESS, null, now);
        saveReplyToOutbox(command.sagaId(), reply.messageId(), reply);
    }

    private void simulateLatency() {
        long delay = properties.processingDelayMs();
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void saveReplyToOutbox(String sagaId, String messageId, Object reply) {
        OutboxEvent outbox = new OutboxEvent();
        outbox.setAggregateId(sagaId);
        outbox.setEventType("PaymentReply");
        outbox.setTopic(KafkaTopics.SAGA_REPLIES);
        outbox.setPayloadJson(objectMapper.writeValueAsString(reply));
        outbox.setCorrelationId(MDC.get("correlationId"));
        outbox.setSagaId(sagaId);
        outbox.setMessageId(messageId);
        outbox.setCreatedAt(LocalDateTime.now());
        outboxRepository.save(outbox);
    }
}
