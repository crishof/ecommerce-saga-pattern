package com.crishof.ecommerce.saga.repository;

import com.crishof.ecommerce.saga.domain.SagaInstance;
import com.crishof.ecommerce.saga.domain.SagaState;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface SagaInstanceRepository extends JpaRepository<SagaInstance, String> {

    Page<SagaInstance> findByState(SagaState state, Pageable pageable);

    long countByState(SagaState state);

    /** Sagas expiradas que aún no han terminado (para el TimeoutScheduler). */
    @Query("SELECT s FROM SagaInstance s WHERE s.expiresAt < :now AND s.completedAt IS NULL")
    List<SagaInstance> findExpiredNotFinal(@Param("now") LocalDateTime now);
}
