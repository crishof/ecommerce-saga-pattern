package com.crishof.ecommerce.shared.command;

import java.time.LocalDateTime;

/**
 * Interfaz marker común a todos los COMANDOS de la saga.
 *
 * Un comando expresa una INTENCIÓN dirigida a un servicio concreto (a diferencia
 * de un evento, que solo notifica que algo ocurrió). Lo emite el orquestador.
 *
 *   sagaId    — agrupa todos los mensajes (comandos + réplicas) de una misma saga
 *   messageId — identificador único del comando, usado para idempotencia
 */
public interface SagaCommand {

    String sagaId();

    String messageId();

    LocalDateTime issuedAt();
}
