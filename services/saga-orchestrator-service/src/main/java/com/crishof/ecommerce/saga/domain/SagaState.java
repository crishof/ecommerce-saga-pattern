package com.crishof.ecommerce.saga.domain;

/**
 * Estados de la máquina de la saga.
 *
 * Camino feliz:
 *   STARTED → RESERVING_STOCK → CHARGING_PAYMENT → CONFIRMING_ORDER
 *           → NOTIFYING_SUCCESS → COMPLETED
 *
 * Camino de compensación (LIFO):
 *   ... → COMPENSATING_PAYMENT → COMPENSATING_STOCK → CANCELLING_ORDER
 *       → NOTIFYING_FAILURE → FAILED
 */
public enum SagaState {
    STARTED,
    RESERVING_STOCK,
    CHARGING_PAYMENT,
    CONFIRMING_ORDER,
    NOTIFYING_SUCCESS,
    COMPLETED,

    COMPENSATING_PAYMENT,
    COMPENSATING_STOCK,
    CANCELLING_ORDER,
    NOTIFYING_FAILURE,
    FAILED;

    public boolean isFinal() {
        return this == COMPLETED || this == FAILED;
    }
}
