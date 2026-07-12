package com.crishof.ecommerce.saga.statemachine;

import com.crishof.ecommerce.saga.domain.SagaInstance;
import com.crishof.ecommerce.saga.domain.SagaState;
import com.crishof.ecommerce.saga.repository.SagaInstanceRepository;
import com.crishof.ecommerce.saga.repository.SagaStepRepository;
import com.crishof.ecommerce.shared.command.CancelOrderCommand;
import com.crishof.ecommerce.shared.command.ChargePaymentCommand;
import com.crishof.ecommerce.shared.command.ConfirmOrderCommand;
import com.crishof.ecommerce.shared.command.RefundPaymentCommand;
import com.crishof.ecommerce.shared.command.ReleaseStockCommand;
import com.crishof.ecommerce.shared.command.ReserveStockCommand;
import com.crishof.ecommerce.shared.command.SendOrderCancelledNotificationCommand;
import com.crishof.ecommerce.shared.command.SendOrderConfirmedNotificationCommand;
import com.crishof.ecommerce.shared.event.OrderPlacedEvent;
import com.crishof.ecommerce.shared.kafka.KafkaTopics;
import com.crishof.ecommerce.shared.reply.NotificationReply;
import com.crishof.ecommerce.shared.reply.OrderStatusUpdateReply;
import com.crishof.ecommerce.shared.reply.PaymentReply;
import com.crishof.ecommerce.shared.reply.ReplyStatus;
import com.crishof.ecommerce.shared.reply.SagaReply;
import com.crishof.ecommerce.shared.reply.StockReservationReply;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * Corazón del proyecto: la máquina de estados que decide qué comando enviar
 * según el estado actual de la saga y el resultado de cada réplica.
 *
 * Cada método público corresponde al handler de un tipo de réplica (o al inicio
 * de la saga). El envío de comandos se delega en {@link SagaCommandDispatcher}.
 */
@Component
public class SagaStateMachine {

    private static final Logger log = LoggerFactory.getLogger(SagaStateMachine.class);

    private final SagaInstanceRepository sagaRepository;
    private final SagaCommandDispatcher dispatcher;
    private final SagaStepRepository stepRepository;
    private final ObjectMapper objectMapper;
    private final long timeoutSeconds;

    public SagaStateMachine(SagaInstanceRepository sagaRepository,
                            SagaCommandDispatcher dispatcher,
                            SagaStepRepository stepRepository,
                            ObjectMapper objectMapper,
                            @Value("${app.saga.timeout-seconds:30}") long timeoutSeconds) {
        this.sagaRepository = sagaRepository;
        this.dispatcher = dispatcher;
        this.stepRepository = stepRepository;
        this.objectMapper = objectMapper;
        this.timeoutSeconds = timeoutSeconds;
    }

    /* ═══════════════════════ Inicio de la saga ═══════════════════════ */

    @Transactional
    public void startSaga(OrderPlacedEvent event, String correlationId) {
        String sagaId = UUID.randomUUID().toString();
        LocalDateTime now = LocalDateTime.now();

        SagaInstance saga = new SagaInstance();
        saga.setSagaId(sagaId);
        saga.setOrderId(event.aggregateId());
        saga.setCustomerId(event.customerId());
        saga.setCustomerEmail(event.customerEmail());
        saga.setTotalAmount(event.totalAmount());
        saga.setLinesJson(objectMapper.writeValueAsString(event.lines()));
        saga.setState(SagaState.STARTED);
        saga.setCorrelationId(correlationId);
        saga.setCreatedAt(now);
        saga.setUpdatedAt(now);
        saga.setExpiresAt(now.plusSeconds(timeoutSeconds));
        sagaRepository.save(saga);

        log.info("[SAGA] {} iniciada para order={}", sagaId, event.aggregateId());

        // Primer paso: reservar stock.
        saga.transitionTo(SagaState.RESERVING_STOCK);
        sagaRepository.save(saga);

        List<ReserveStockCommand.StockReservationRequest> reservations = event.lines().stream()
                .map(l -> new ReserveStockCommand.StockReservationRequest(l.productId(), l.quantity()))
                .toList();

        ReserveStockCommand command = new ReserveStockCommand(
                sagaId, UUID.randomUUID().toString(),
                event.aggregateId(), reservations, LocalDateTime.now());

        dispatcher.dispatch(command, KafkaTopics.INVENTORY_COMMANDS, "ReserveStockCommand",
                sagaId, saga.getCorrelationId(), "RESERVE_STOCK", 1);
    }

    /* ═══════════════════════ Handlers de réplicas ═══════════════════════ */

    @Transactional
    public void handleStockReservationReply(StockReservationReply reply) {
        SagaInstance saga = findSagaOrThrow(reply.sagaId());
        recordReply(reply.replyToMessageId(), reply);

        if (saga.getState().isFinal()) {
            logIgnored(saga);
            return;
        }

        // Réplica de una LIBERACIÓN de stock (compensación): avanzar a cancelar order.
        if (saga.getState() == SagaState.COMPENSATING_STOCK) {
            proceedToCancelOrder(saga);
            return;
        }

        if (reply.status() == ReplyStatus.SUCCESS) {
            saga.transitionTo(SagaState.CHARGING_PAYMENT);
            sagaRepository.save(saga);

            ChargePaymentCommand command = new ChargePaymentCommand(
                    saga.getSagaId(), UUID.randomUUID().toString(),
                    saga.getOrderId(), saga.getCustomerId(),
                    saga.getTotalAmount(), LocalDateTime.now());

            dispatcher.dispatch(command, KafkaTopics.PAYMENT_COMMANDS, "ChargePaymentCommand",
                    saga.getSagaId(), saga.getCorrelationId(), "CHARGE_PAYMENT", 2);
        } else {
            // No se pudo reservar: nada que reembolsar ni liberar → cancelar order.
            saga.setFailureReason("Stock reservation failed: " + reply.failureReason());
            startCompensation(saga, "stock");
        }
    }

    @Transactional
    public void handlePaymentReply(PaymentReply reply) {
        SagaInstance saga = findSagaOrThrow(reply.sagaId());
        recordReply(reply.replyToMessageId(), reply);

        if (saga.getState().isFinal()) {
            logIgnored(saga);
            return;
        }

        // Réplica de un REEMBOLSO (compensación): avanzar a liberar stock.
        if (saga.getState() == SagaState.COMPENSATING_PAYMENT) {
            proceedToCompensateStock(saga);
            return;
        }

        if (reply.status() == ReplyStatus.SUCCESS) {
            saga.transitionTo(SagaState.CONFIRMING_ORDER);
            sagaRepository.save(saga);

            ConfirmOrderCommand command = new ConfirmOrderCommand(
                    saga.getSagaId(), UUID.randomUUID().toString(),
                    saga.getOrderId(), LocalDateTime.now());

            dispatcher.dispatch(command, KafkaTopics.ORDER_COMMANDS, "ConfirmOrderCommand",
                    saga.getSagaId(), saga.getCorrelationId(), "CONFIRM_ORDER", 3);
        } else {
            saga.setFailureReason("Payment failed: " + reply.failureReason());
            startCompensation(saga, "payment");
        }
    }

    @Transactional
    public void handleOrderStatusUpdateReply(OrderStatusUpdateReply reply) {
        SagaInstance saga = findSagaOrThrow(reply.sagaId());
        recordReply(reply.replyToMessageId(), reply);

        if (saga.getState().isFinal()) {
            logIgnored(saga);
            return;
        }

        switch (saga.getState()) {
            case CONFIRMING_ORDER -> {
                saga.transitionTo(SagaState.NOTIFYING_SUCCESS);
                sagaRepository.save(saga);
                dispatchConfirmedNotification(saga);
            }
            case CANCELLING_ORDER -> {
                saga.transitionTo(SagaState.NOTIFYING_FAILURE);
                sagaRepository.save(saga);
                dispatchCancelledNotification(saga);
            }
            default -> log.warn("[SAGA] {} OrderStatusUpdateReply inesperado en estado {}",
                    saga.getSagaId(), saga.getState());
        }
    }

    @Transactional
    public void handleNotificationReply(NotificationReply reply) {
        SagaInstance saga = findSagaOrThrow(reply.sagaId());
        recordReply(reply.replyToMessageId(), reply);

        if (saga.getState().isFinal()) {
            logIgnored(saga);
            return;
        }

        switch (saga.getState()) {
            case NOTIFYING_SUCCESS -> saga.transitionTo(SagaState.COMPLETED);
            case NOTIFYING_FAILURE -> saga.transitionTo(SagaState.FAILED);
            default -> {
                log.warn("[SAGA] {} NotificationReply inesperado en estado {}",
                        saga.getSagaId(), saga.getState());
                return;
            }
        }
        sagaRepository.save(saga);
        log.info("[SAGA] {} finalizada con estado {}", saga.getSagaId(), saga.getState());
    }

    /* ═══════════════════════ Timeout / compensación forzada ═══════════════════════ */

    /**
     * Fuerza la compensación de una saga expirada según su estado actual.
     * No rompe sagas que ya están confirmando/notificando (a punto de terminar):
     * el timeout es una salvaguarda, no un martillo.
     */
    @Transactional
    public void forceCompensate(String sagaId) {
        SagaInstance saga = findSagaOrThrow(sagaId);
        if (saga.getState().isFinal()) {
            return;
        }
        saga.setFailureReason("Saga timeout en estado " + saga.getState());
        log.warn("[SAGA] {} expirada en estado {}, forzando compensación",
                saga.getSagaId(), saga.getState());

        switch (saga.getState()) {
            case STARTED, RESERVING_STOCK -> proceedToCancelOrder(saga);
            case CHARGING_PAYMENT -> startCompensation(saga, "payment");
            default -> log.info("[SAGA] {} en estado {}: se deja completar de forma natural",
                    saga.getSagaId(), saga.getState());
        }
    }

    /* ═══════════════════════ Pasos internos ═══════════════════════ */

    private void startCompensation(SagaInstance saga, String failedStep) {
        log.warn("[SAGA] {} entrando en compensación (fallo en {}: {})",
                saga.getSagaId(), failedStep, saga.getFailureReason());

        if ("payment".equals(failedStep)) {
            // Hubo (intento de) cobro → reembolsar, luego liberar stock, cancelar y notificar.
            saga.transitionTo(SagaState.COMPENSATING_PAYMENT);
            sagaRepository.save(saga);

            RefundPaymentCommand command = new RefundPaymentCommand(
                    saga.getSagaId(), UUID.randomUUID().toString(),
                    saga.getOrderId(), saga.getFailureReason(), LocalDateTime.now());

            dispatcher.dispatch(command, KafkaTopics.PAYMENT_COMMANDS, "RefundPaymentCommand",
                    saga.getSagaId(), saga.getCorrelationId(), "REFUND_PAYMENT", 10);
        } else {
            // Fallo en la reserva: no hubo pago, saltamos directo a cancelar el pedido.
            proceedToCancelOrder(saga);
        }
    }

    private void proceedToCompensateStock(SagaInstance saga) {
        saga.transitionTo(SagaState.COMPENSATING_STOCK);
        sagaRepository.save(saga);

        ReleaseStockCommand command = new ReleaseStockCommand(
                saga.getSagaId(), UUID.randomUUID().toString(),
                saga.getOrderId(), saga.getFailureReason(), LocalDateTime.now());

        dispatcher.dispatch(command, KafkaTopics.INVENTORY_COMMANDS, "ReleaseStockCommand",
                saga.getSagaId(), saga.getCorrelationId(), "RELEASE_STOCK", 11);
    }

    private void proceedToCancelOrder(SagaInstance saga) {
        saga.transitionTo(SagaState.CANCELLING_ORDER);
        sagaRepository.save(saga);

        CancelOrderCommand command = new CancelOrderCommand(
                saga.getSagaId(), UUID.randomUUID().toString(),
                saga.getOrderId(), saga.getFailureReason(), LocalDateTime.now());

        dispatcher.dispatch(command, KafkaTopics.ORDER_COMMANDS, "CancelOrderCommand",
                saga.getSagaId(), saga.getCorrelationId(), "CANCEL_ORDER", 12);
    }

    private void dispatchConfirmedNotification(SagaInstance saga) {
        SendOrderConfirmedNotificationCommand command = new SendOrderConfirmedNotificationCommand(
                saga.getSagaId(), UUID.randomUUID().toString(),
                saga.getOrderId(), saga.getCustomerId(),
                saga.getCustomerEmail(), saga.getTotalAmount(), LocalDateTime.now());

        dispatcher.dispatch(command, KafkaTopics.NOTIFICATION_COMMANDS,
                "SendOrderConfirmedNotificationCommand",
                saga.getSagaId(), saga.getCorrelationId(), "NOTIFY_SUCCESS", 4);
    }

    private void dispatchCancelledNotification(SagaInstance saga) {
        SendOrderCancelledNotificationCommand command = new SendOrderCancelledNotificationCommand(
                saga.getSagaId(), UUID.randomUUID().toString(),
                saga.getOrderId(), saga.getCustomerId(),
                saga.getCustomerEmail(), saga.getFailureReason(), LocalDateTime.now());

        dispatcher.dispatch(command, KafkaTopics.NOTIFICATION_COMMANDS,
                "SendOrderCancelledNotificationCommand",
                saga.getSagaId(), saga.getCorrelationId(), "NOTIFY_FAILURE", 13);
    }

    private SagaInstance findSagaOrThrow(String sagaId) {
        return sagaRepository.findById(sagaId)
                .orElseThrow(() -> new IllegalStateException("Saga not found: " + sagaId));
    }

    private void recordReply(String replyToMessageId, SagaReply reply) {
        stepRepository.findByMessageId(replyToMessageId).ifPresent(step -> {
            try {
                step.setReplyStatus(reply.status().name());
                step.setReplyJson(objectMapper.writeValueAsString(reply));
                step.setRepliedAt(LocalDateTime.now());
                stepRepository.save(step);
            } catch (Exception e) {
                log.error("Error registrando reply del step {}", step.getId(), e);
            }
        });
    }

    private void logIgnored(SagaInstance saga) {
        log.warn("[SAGA] {} ya está en estado final {}, réplica ignorada",
                saga.getSagaId(), saga.getState());
    }
}
