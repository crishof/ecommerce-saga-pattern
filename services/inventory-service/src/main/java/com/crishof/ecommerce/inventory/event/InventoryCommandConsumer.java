package com.crishof.ecommerce.inventory.event;

import com.crishof.ecommerce.inventory.domain.ProcessedMessage;
import com.crishof.ecommerce.inventory.repository.ProcessedMessageRepository;
import com.crishof.ecommerce.inventory.service.InventoryService;
import com.crishof.ecommerce.shared.command.ReleaseStockCommand;
import com.crishof.ecommerce.shared.command.ReserveStockCommand;
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
 * Consume inventory-commands del orquestador (ReserveStock / ReleaseStock) y
 * responde con StockReservationReply. Idempotencia vía processed_messages.
 */
@Component
public class InventoryCommandConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandConsumer.class);

    private final InventoryService inventoryService;
    private final ProcessedMessageRepository processedRepository;
    private final ObjectMapper objectMapper;

    public InventoryCommandConsumer(InventoryService inventoryService,
                                    ProcessedMessageRepository processedRepository,
                                    ObjectMapper objectMapper) {
        this.inventoryService = inventoryService;
        this.processedRepository = processedRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopics.INVENTORY_COMMANDS, groupId = "inventory-service.inventory-commands")
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
                log.debug("[INVENTORY] Mensaje {} ya procesado, ignorando", messageId);
                return;
            }

            switch (messageType) {
                case "ReserveStockCommand" ->
                        inventoryService.reserve(objectMapper.readValue(payload, ReserveStockCommand.class));
                case "ReleaseStockCommand" ->
                        inventoryService.release(objectMapper.readValue(payload, ReleaseStockCommand.class));
                default -> log.warn("[INVENTORY] Comando no soportado: {}", messageType);
            }

            processedRepository.save(new ProcessedMessage(messageId, LocalDateTime.now()));
        } finally {
            MDC.remove("correlationId");
            MDC.remove("sagaId");
        }
    }
}
