package com.crishof.ecommerce.saga.controller;

import com.crishof.ecommerce.saga.domain.SagaInstance;
import com.crishof.ecommerce.saga.domain.SagaState;
import com.crishof.ecommerce.saga.domain.SagaStep;
import com.crishof.ecommerce.saga.repository.SagaInstanceRepository;
import com.crishof.ecommerce.saga.repository.SagaStepRepository;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Endpoints de introspección de sagas. DIFERENCIAL clave frente al proyecto 19:
 * en choreography no había un lugar donde ver el flujo completo; aquí es un query.
 *
 *   GET /api/sagas                → lista paginada (filtro opcional por estado)
 *   GET /api/sagas/{sagaId}       → detalle con el histórico de pasos
 *   GET /api/sagas/stats          → conteo por estado (dashboards)
 */
@RestController
@RequestMapping("/api/sagas")
public class SagaController {

    private final SagaInstanceRepository sagaRepository;
    private final SagaStepRepository stepRepository;

    public SagaController(SagaInstanceRepository sagaRepository,
                          SagaStepRepository stepRepository) {
        this.sagaRepository = sagaRepository;
        this.stepRepository = stepRepository;
    }

    @GetMapping
    public ResponseEntity<Map<String, Object>> listSagas(
            @RequestParam(required = false) String state,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        Pageable pageable = PageRequest.of(page, size, Sort.by("createdAt").descending());
        Page<SagaInstance> sagas = (state != null)
                ? sagaRepository.findByState(SagaState.valueOf(state), pageable)
                : sagaRepository.findAll(pageable);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("content", sagas.getContent().stream().map(this::toSummary).toList());
        body.put("page", sagas.getNumber());
        body.put("size", sagas.getSize());
        body.put("totalElements", sagas.getTotalElements());
        body.put("totalPages", sagas.getTotalPages());
        return ResponseEntity.ok(body);
    }

    @GetMapping("/{sagaId}")
    public ResponseEntity<Map<String, Object>> getSagaDetail(@PathVariable String sagaId) {
        SagaInstance saga = sagaRepository.findById(sagaId)
                .orElseThrow(() -> new IllegalArgumentException("Saga not found: " + sagaId));
        List<SagaStep> steps = stepRepository.findBySagaIdOrderByStepNumber(sagaId);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sagaId", saga.getSagaId());
        body.put("orderId", saga.getOrderId());
        body.put("state", saga.getState().name());
        body.put("customerId", saga.getCustomerId());
        body.put("customerEmail", saga.getCustomerEmail());
        body.put("totalAmount", saga.getTotalAmount());
        body.put("failureReason", saga.getFailureReason() != null ? saga.getFailureReason() : "");
        body.put("createdAt", saga.getCreatedAt());
        body.put("updatedAt", saga.getUpdatedAt());
        body.put("completedAt", saga.getCompletedAt() != null ? saga.getCompletedAt() : "");
        body.put("steps", steps.stream().map(this::toStepMap).toList());
        return ResponseEntity.ok(body);
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> stats() {
        Map<String, Long> countsByState = new LinkedHashMap<>();
        for (SagaState state : SagaState.values()) {
            countsByState.put(state.name(), sagaRepository.countByState(state));
        }
        Map<String, Object> body = new HashMap<>();
        body.put("countsByState", countsByState);
        return ResponseEntity.ok(body);
    }

    private Map<String, Object> toSummary(SagaInstance saga) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("sagaId", saga.getSagaId());
        m.put("orderId", saga.getOrderId());
        m.put("state", saga.getState().name());
        m.put("customerId", saga.getCustomerId());
        m.put("totalAmount", saga.getTotalAmount());
        m.put("createdAt", saga.getCreatedAt());
        return m;
    }

    private Map<String, Object> toStepMap(SagaStep step) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("stepNumber", step.getStepNumber());
        m.put("stepType", step.getStepType());
        m.put("commandType", step.getCommandType());
        m.put("replyStatus", step.getReplyStatus() != null ? step.getReplyStatus() : "PENDING");
        m.put("dispatchedAt", step.getDispatchedAt());
        m.put("repliedAt", step.getRepliedAt() != null ? step.getRepliedAt() : "");
        return m;
    }
}
