package com.crishof.ecommerce.saga.scheduler;

import com.crishof.ecommerce.saga.domain.SagaInstance;
import com.crishof.ecommerce.saga.repository.SagaInstanceRepository;
import com.crishof.ecommerce.saga.statemachine.SagaStateMachine;
import java.time.LocalDateTime;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Cada 5s revisa sagas expiradas (past expires_at) que aún no están en estado
 * final y fuerza su compensación. Es el mecanismo de resiliencia frente a
 * servicios caídos: si una réplica nunca llega, la saga no se queda a medias.
 */
@Component
public class TimeoutScheduler {

    private static final Logger log = LoggerFactory.getLogger(TimeoutScheduler.class);

    private final SagaInstanceRepository sagaRepository;
    private final SagaStateMachine stateMachine;

    public TimeoutScheduler(SagaInstanceRepository sagaRepository,
                            SagaStateMachine stateMachine) {
        this.sagaRepository = sagaRepository;
        this.stateMachine = stateMachine;
    }

    @Scheduled(fixedDelayString = "${app.saga.poll-interval-ms:5000}")
    public void handleTimeouts() {
        List<SagaInstance> expired = sagaRepository.findExpiredNotFinal(LocalDateTime.now());
        for (SagaInstance saga : expired) {
            try {
                stateMachine.forceCompensate(saga.getSagaId());
            } catch (Exception e) {
                log.error("[SAGA] Error compensando saga expirada {}", saga.getSagaId(), e);
            }
        }
    }
}
