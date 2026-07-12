package com.crishof.ecommerce.saga.repository;

import com.crishof.ecommerce.saga.domain.SagaStep;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaStepRepository extends JpaRepository<SagaStep, Long> {

    List<SagaStep> findBySagaIdOrderByStepNumber(String sagaId);

    Optional<SagaStep> findByMessageId(String messageId);
}
