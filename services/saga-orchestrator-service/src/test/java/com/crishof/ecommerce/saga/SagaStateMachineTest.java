package com.crishof.ecommerce.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.crishof.ecommerce.saga.domain.SagaInstance;
import com.crishof.ecommerce.saga.domain.SagaState;
import com.crishof.ecommerce.saga.repository.SagaInstanceRepository;
import com.crishof.ecommerce.saga.repository.SagaStepRepository;
import com.crishof.ecommerce.saga.statemachine.SagaCommandDispatcher;
import com.crishof.ecommerce.saga.statemachine.SagaStateMachine;
import com.crishof.ecommerce.shared.command.CancelOrderCommand;
import com.crishof.ecommerce.shared.command.ChargePaymentCommand;
import com.crishof.ecommerce.shared.command.ConfirmOrderCommand;
import com.crishof.ecommerce.shared.command.RefundPaymentCommand;
import com.crishof.ecommerce.shared.command.ReserveStockCommand;
import com.crishof.ecommerce.shared.command.SagaCommand;
import com.crishof.ecommerce.shared.command.SendOrderConfirmedNotificationCommand;
import com.crishof.ecommerce.shared.event.OrderPlacedEvent;
import com.crishof.ecommerce.shared.reply.NotificationReply;
import com.crishof.ecommerce.shared.reply.OrderStatusUpdateReply;
import com.crishof.ecommerce.shared.reply.PaymentReply;
import com.crishof.ecommerce.shared.reply.ReplyStatus;
import com.crishof.ecommerce.shared.reply.StockReservationReply;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Verifica las transiciones de la máquina de estados con un dispatcher falso.
 * Es el test más valioso: prueba la lógica NUEVA del proyecto 20.
 */
@ExtendWith(MockitoExtension.class)
class SagaStateMachineTest {

    @Mock
    SagaInstanceRepository sagaRepository;
    @Mock
    SagaCommandDispatcher dispatcher;
    @Mock
    SagaStepRepository stepRepository;

    private final ObjectMapper objectMapper = JsonMapper.builder().build();

    private SagaStateMachine machine() {
        return new SagaStateMachine(sagaRepository, dispatcher, stepRepository, objectMapper, 30);
    }

    private SagaInstance sagaInState(SagaState state) {
        SagaInstance saga = new SagaInstance();
        saga.setSagaId("saga-1");
        saga.setOrderId("o1");
        saga.setCustomerId(2L);
        saga.setCustomerEmail("alice@shop.com");
        saga.setTotalAmount(new BigDecimal("99.99"));
        saga.setLinesJson("[]");
        saga.setState(state);
        saga.setCreatedAt(LocalDateTime.now());
        saga.setUpdatedAt(LocalDateTime.now());
        saga.setExpiresAt(LocalDateTime.now().plusSeconds(30));
        return saga;
    }

    private void stubSaga(SagaInstance saga) {
        when(sagaRepository.findById("saga-1")).thenReturn(Optional.of(saga));
    }

    private <T extends SagaCommand> T verifyDispatched(String commandType, Class<T> type) {
        ArgumentCaptor<SagaCommand> captor = ArgumentCaptor.forClass(SagaCommand.class);
        verify(dispatcher).dispatch(captor.capture(), any(), eq(commandType),
                any(), any(), any(), anyInt());
        assertThat(captor.getValue()).isInstanceOf(type);
        return type.cast(captor.getValue());
    }

    @Test
    void startSagaReservesStock() {
        OrderPlacedEvent event = new OrderPlacedEvent(
                "o1", 2L, "alice@shop.com", "Alice",
                List.of(new OrderPlacedEvent.OrderLineData("10", "Laptop", new BigDecimal("99.99"), 1)),
                new BigDecimal("99.99"), LocalDateTime.now(), "corr-1");

        machine().startSaga(event, "corr-1");

        ArgumentCaptor<SagaInstance> saved = ArgumentCaptor.forClass(SagaInstance.class);
        verify(sagaRepository, org.mockito.Mockito.atLeastOnce()).save(saved.capture());
        assertThat(saved.getValue().getState()).isEqualTo(SagaState.RESERVING_STOCK);
        verifyDispatched("ReserveStockCommand", ReserveStockCommand.class);
    }

    @Test
    void stockReservedAdvancesToCharging() {
        SagaInstance saga = sagaInState(SagaState.RESERVING_STOCK);
        stubSaga(saga);

        machine().handleStockReservationReply(new StockReservationReply(
                "saga-1", "m2", "m1", "o1", ReplyStatus.SUCCESS, null, LocalDateTime.now()));

        assertThat(saga.getState()).isEqualTo(SagaState.CHARGING_PAYMENT);
        verifyDispatched("ChargePaymentCommand", ChargePaymentCommand.class);
    }

    @Test
    void stockReservationFailureCancelsOrder() {
        SagaInstance saga = sagaInState(SagaState.RESERVING_STOCK);
        stubSaga(saga);

        machine().handleStockReservationReply(new StockReservationReply(
                "saga-1", "m2", "m1", "o1", ReplyStatus.FAILURE, "sin stock", LocalDateTime.now()));

        assertThat(saga.getState()).isEqualTo(SagaState.CANCELLING_ORDER);
        verifyDispatched("CancelOrderCommand", CancelOrderCommand.class);
    }

    @Test
    void paymentSuccessConfirmsOrder() {
        SagaInstance saga = sagaInState(SagaState.CHARGING_PAYMENT);
        stubSaga(saga);

        machine().handlePaymentReply(new PaymentReply(
                "saga-1", "m3", "m2", "o1", "pay-1", "TX-1", ReplyStatus.SUCCESS, null, LocalDateTime.now()));

        assertThat(saga.getState()).isEqualTo(SagaState.CONFIRMING_ORDER);
        verifyDispatched("ConfirmOrderCommand", ConfirmOrderCommand.class);
    }

    @Test
    void paymentFailureStartsRefund() {
        SagaInstance saga = sagaInState(SagaState.CHARGING_PAYMENT);
        stubSaga(saga);

        machine().handlePaymentReply(new PaymentReply(
                "saga-1", "m3", "m2", "o1", "pay-1", null, ReplyStatus.FAILURE, "rechazado", LocalDateTime.now()));

        assertThat(saga.getState()).isEqualTo(SagaState.COMPENSATING_PAYMENT);
        verifyDispatched("RefundPaymentCommand", RefundPaymentCommand.class);
    }

    @Test
    void orderConfirmedTriggersSuccessNotification() {
        SagaInstance saga = sagaInState(SagaState.CONFIRMING_ORDER);
        stubSaga(saga);

        machine().handleOrderStatusUpdateReply(new OrderStatusUpdateReply(
                "saga-1", "m4", "m3", "o1", "PAID", ReplyStatus.SUCCESS, null, LocalDateTime.now()));

        assertThat(saga.getState()).isEqualTo(SagaState.NOTIFYING_SUCCESS);
        verifyDispatched("SendOrderConfirmedNotificationCommand", SendOrderConfirmedNotificationCommand.class);
    }

    @Test
    void notificationSuccessCompletesSaga() {
        SagaInstance saga = sagaInState(SagaState.NOTIFYING_SUCCESS);
        stubSaga(saga);

        machine().handleNotificationReply(new NotificationReply(
                "saga-1", "m5", "m4", ReplyStatus.SUCCESS, null, LocalDateTime.now()));

        assertThat(saga.getState()).isEqualTo(SagaState.COMPLETED);
        assertThat(saga.getCompletedAt()).isNotNull();
    }

    @Test
    void notificationFailureBranchEndsInFailed() {
        SagaInstance saga = sagaInState(SagaState.NOTIFYING_FAILURE);
        stubSaga(saga);

        machine().handleNotificationReply(new NotificationReply(
                "saga-1", "m5", "m4", ReplyStatus.SUCCESS, null, LocalDateTime.now()));

        assertThat(saga.getState()).isEqualTo(SagaState.FAILED);
        assertThat(saga.getCompletedAt()).isNotNull();
    }

    @Test
    void replyInFinalStateIsIgnored() {
        SagaInstance saga = sagaInState(SagaState.COMPLETED);
        stubSaga(saga);

        machine().handlePaymentReply(new PaymentReply(
                "saga-1", "m9", "m2", "o1", "pay-1", "TX-1", ReplyStatus.SUCCESS, null, LocalDateTime.now()));

        assertThat(saga.getState()).isEqualTo(SagaState.COMPLETED);
        verify(dispatcher, never()).dispatch(any(), any(), any(), any(), any(), any(), anyInt());
    }
}
