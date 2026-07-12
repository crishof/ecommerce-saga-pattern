package com.crishof.ecommerce.saga.statemachine;

import com.crishof.ecommerce.saga.domain.SagaStep;
import com.crishof.ecommerce.saga.outbox.OutboxEvent;
import com.crishof.ecommerce.saga.outbox.OutboxRepository;
import com.crishof.ecommerce.saga.repository.SagaStepRepository;
import com.crishof.ecommerce.shared.command.SagaCommand;
import java.time.LocalDateTime;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

/**
 * Encapsula el envío de comandos vía outbox y registra el paso en saga_steps
 * (trazabilidad). Se ejecuta dentro de la transacción del handler de la saga,
 * de modo que la transición de estado, el paso y el comando se persisten atómicamente.
 */
@Component
public class SagaCommandDispatcher {

    private final OutboxRepository outboxRepository;
    private final SagaStepRepository stepRepository;
    private final ObjectMapper objectMapper;

    public SagaCommandDispatcher(OutboxRepository outboxRepository,
                                 SagaStepRepository stepRepository,
                                 ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.stepRepository = stepRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void dispatch(SagaCommand command, String topic, String commandType,
                         String sagaId, String correlationId,
                         String stepType, int stepNumber) {
        LocalDateTime now = LocalDateTime.now();
        String commandJson = objectMapper.writeValueAsString(command);

        SagaStep step = new SagaStep();
        step.setSagaId(sagaId);
        step.setStepNumber(stepNumber);
        step.setStepType(stepType);
        step.setMessageId(command.messageId());
        step.setCommandType(commandType);
        step.setCommandJson(commandJson);
        step.setDispatchedAt(now);
        stepRepository.save(step);

        OutboxEvent outbox = new OutboxEvent();
        outbox.setAggregateId(sagaId);
        outbox.setEventType(commandType);
        outbox.setTopic(topic);
        outbox.setPayloadJson(commandJson);
        outbox.setSagaId(sagaId);
        outbox.setMessageId(command.messageId());
        outbox.setCorrelationId(correlationId);
        outbox.setCreatedAt(now);
        outboxRepository.save(outbox);
    }
}
