package com.crishof.ecommerce.notification;

import static org.assertj.core.api.Assertions.assertThat;

import com.crishof.ecommerce.notification.domain.Notification;
import com.crishof.ecommerce.notification.outbox.OutboxEvent;
import com.crishof.ecommerce.notification.outbox.OutboxRepository;
import com.crishof.ecommerce.notification.repository.NotificationRepository;
import com.crishof.ecommerce.notification.service.NotificationService;
import com.crishof.ecommerce.shared.command.SendOrderCancelledNotificationCommand;
import com.crishof.ecommerce.shared.command.SendOrderConfirmedNotificationCommand;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {

    @Mock
    NotificationRepository notificationRepository;
    @Mock
    OutboxRepository outboxRepository;

    private final ObjectMapper objectMapper = JsonMapper.builder().build();

    private NotificationService service() {
        return new NotificationService(notificationRepository, outboxRepository, objectMapper);
    }

    @Test
    void confirmedStoresSuccessNotificationAndReplies() {
        service().onOrderConfirmed(new SendOrderConfirmedNotificationCommand(
                "saga-1", "msg-1", "o1", 2L, "alice@shop.com",
                new BigDecimal("99.99"), LocalDateTime.now()));

        ArgumentCaptor<Notification> notif = ArgumentCaptor.forClass(Notification.class);
        Mockito.verify(notificationRepository).save(notif.capture());
        assertThat(notif.getValue().getType()).isEqualTo("ORDER_CONFIRMED");
        assertThat(notif.getValue().getCustomerEmail()).isEqualTo("alice@shop.com");

        ArgumentCaptor<OutboxEvent> reply = ArgumentCaptor.forClass(OutboxEvent.class);
        Mockito.verify(outboxRepository).save(reply.capture());
        assertThat(reply.getValue().getEventType()).isEqualTo("NotificationReply");
        assertThat(reply.getValue().getTopic()).isEqualTo("saga-replies");
    }

    @Test
    void cancelledStoresFailureNotificationAndReplies() {
        service().onOrderCancelled(new SendOrderCancelledNotificationCommand(
                "saga-1", "msg-2", "o1", 2L, "alice@shop.com",
                "Pago rechazado", LocalDateTime.now()));

        ArgumentCaptor<Notification> notif = ArgumentCaptor.forClass(Notification.class);
        Mockito.verify(notificationRepository).save(notif.capture());
        assertThat(notif.getValue().getType()).isEqualTo("ORDER_CANCELLED");
        assertThat(notif.getValue().getBody()).contains("Pago rechazado");

        Mockito.verify(outboxRepository).save(org.mockito.ArgumentMatchers.any(OutboxEvent.class));
    }
}
