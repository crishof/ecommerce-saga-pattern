package com.crishof.ecommerce.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.crishof.ecommerce.inventory.domain.StockReservation;
import com.crishof.ecommerce.inventory.outbox.OutboxEvent;
import com.crishof.ecommerce.inventory.outbox.OutboxRepository;
import com.crishof.ecommerce.inventory.repository.StockReservationRepository;
import com.crishof.ecommerce.inventory.service.InventoryService;
import com.crishof.ecommerce.shared.command.ReleaseStockCommand;
import com.crishof.ecommerce.shared.command.ReserveStockCommand;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    StockReservationRepository reservationRepository;
    @Mock
    OutboxRepository outboxRepository;

    private final ObjectMapper objectMapper = JsonMapper.builder().build();

    private InventoryService service() {
        return new InventoryService(reservationRepository, outboxRepository, objectMapper);
    }

    private ReserveStockCommand reserveCommand() {
        return new ReserveStockCommand("saga-1", "msg-1", "o1",
                List.of(new ReserveStockCommand.StockReservationRequest("10", 2)),
                LocalDateTime.now());
    }

    @Test
    void reserveCreatesReservationsAndRepliesSuccess() {
        when(reservationRepository.existsByOrderId("o1")).thenReturn(false);

        service().reserve(reserveCommand());

        verify(reservationRepository, times(1)).save(org.mockito.ArgumentMatchers.any(StockReservation.class));
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo("StockReservationReply");
        assertThat(captor.getValue().getTopic()).isEqualTo("saga-replies");
        assertThat(captor.getValue().getPayloadJson()).contains("SUCCESS");
    }

    @Test
    void reserveSkipsDuplicateOrderButStillReplies() {
        when(reservationRepository.existsByOrderId("o1")).thenReturn(true);

        service().reserve(reserveCommand());

        // No se duplican reservas, pero SÍ se responde (idempotencia de la réplica).
        verify(reservationRepository, org.mockito.Mockito.never())
                .save(org.mockito.ArgumentMatchers.any(StockReservation.class));
        verify(outboxRepository).save(org.mockito.ArgumentMatchers.any(OutboxEvent.class));
    }

    @Test
    void releaseMarksReservationsReleasedAndReplies() {
        StockReservation held = new StockReservation();
        held.setOrderId("o1");
        held.setProductId("10");
        held.setQuantity(2);
        held.setStatus("HELD");
        held.setCreatedAt(LocalDateTime.now());
        when(reservationRepository.findByOrderId("o1")).thenReturn(List.of(held));

        service().release(new ReleaseStockCommand("saga-1", "msg-2", "o1", "payment failed",
                LocalDateTime.now()));

        assertThat(held.getStatus()).isEqualTo("RELEASED");
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo("StockReservationReply");
    }
}
