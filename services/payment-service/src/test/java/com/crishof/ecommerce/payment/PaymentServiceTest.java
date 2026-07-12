package com.crishof.ecommerce.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.crishof.ecommerce.payment.config.PaymentsProperties;
import com.crishof.ecommerce.payment.domain.Payment;
import com.crishof.ecommerce.payment.outbox.OutboxEvent;
import com.crishof.ecommerce.payment.outbox.OutboxRepository;
import com.crishof.ecommerce.payment.repository.PaymentRepository;
import com.crishof.ecommerce.payment.service.PaymentService;
import com.crishof.ecommerce.shared.command.ChargePaymentCommand;
import com.crishof.ecommerce.shared.command.RefundPaymentCommand;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    PaymentRepository paymentRepository;
    @Mock
    OutboxRepository outboxRepository;

    private final ObjectMapper objectMapper = JsonMapper.builder().build();

    private PaymentService service(double failureRate) {
        return new PaymentService(paymentRepository, outboxRepository,
                objectMapper, new PaymentsProperties(failureRate, 0));
    }

    private ChargePaymentCommand chargeCommand() {
        return new ChargePaymentCommand("saga-1", "msg-1", "o1", 2L,
                new BigDecimal("99.99"), LocalDateTime.now());
    }

    @Test
    void chargeSucceedsWithZeroFailureRate() {
        service(0.0).charge(chargeCommand());

        verify(paymentRepository).save(org.mockito.ArgumentMatchers.any(Payment.class));
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo("PaymentReply");
        assertThat(captor.getValue().getTopic()).isEqualTo("saga-replies");
        assertThat(captor.getValue().getSagaId()).isEqualTo("saga-1");
        assertThat(captor.getValue().getPayloadJson()).contains("SUCCESS");
    }

    @Test
    void chargeFailsWithFullFailureRate() {
        service(1.0).charge(chargeCommand());

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getPayloadJson()).contains("FAILURE");
    }

    @Test
    void refundMarksPaymentRefundedAndReplies() {
        Payment paid = new Payment();
        paid.setId("pay-1");
        paid.setOrderId("o1");
        paid.setStatus("SUCCESS");
        paid.setAmount(new BigDecimal("99.99"));
        paid.setCustomerId(2L);
        paid.setProcessedAt(LocalDateTime.now());
        when(paymentRepository.findByOrderId("o1")).thenReturn(Optional.of(paid));

        service(0.0).refund(new RefundPaymentCommand(
                "saga-1", "msg-2", "o1", "payment failed", LocalDateTime.now()));

        assertThat(paid.getStatus()).isEqualTo("REFUNDED");
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo("PaymentReply");
        assertThat(captor.getValue().getPayloadJson()).contains("SUCCESS");
    }
}
