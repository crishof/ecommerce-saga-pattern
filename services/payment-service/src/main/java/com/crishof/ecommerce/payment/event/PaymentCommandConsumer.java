package com.crishof.ecommerce.payment.event;

import com.crishof.ecommerce.payment.domain.ProcessedMessage;
import com.crishof.ecommerce.payment.repository.ProcessedMessageRepository;
import com.crishof.ecommerce.payment.service.PaymentService;
import com.crishof.ecommerce.shared.command.ChargePaymentCommand;
import com.crishof.ecommerce.shared.command.RefundPaymentCommand;
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
 * Consume payment-commands del orquestador (ChargePayment / RefundPayment) y
 * responde con PaymentReply. Idempotencia vía processed_messages.
 */
@Component
public class PaymentCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandConsumer.class);

    private final PaymentService paymentService;
    private final ProcessedMessageRepository processedRepository;
    private final ObjectMapper objectMapper;

    public PaymentCommandConsumer(PaymentService paymentService,
                                  ProcessedMessageRepository processedRepository,
                                  ObjectMapper objectMapper) {
        this.paymentService = paymentService;
        this.processedRepository = processedRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_COMMANDS, groupId = "payment-service.payment-commands")
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
                log.debug("[PAYMENT] Mensaje {} ya procesado, ignorando", messageId);
                return;
            }

            switch (messageType) {
                case "ChargePaymentCommand" ->
                        paymentService.charge(objectMapper.readValue(payload, ChargePaymentCommand.class));
                case "RefundPaymentCommand" ->
                        paymentService.refund(objectMapper.readValue(payload, RefundPaymentCommand.class));
                default -> log.warn("[PAYMENT] Comando no soportado: {}", messageType);
            }

            processedRepository.save(new ProcessedMessage(messageId, LocalDateTime.now()));
        } finally {
            MDC.remove("correlationId");
            MDC.remove("sagaId");
        }
    }
}
