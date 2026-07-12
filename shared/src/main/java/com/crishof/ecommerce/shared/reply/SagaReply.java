package com.crishof.ecommerce.shared.reply;

import java.time.LocalDateTime;

/**
 * Interfaz marker común a todas las RÉPLICAS de la saga.
 *
 * Una réplica es la respuesta de un servicio a un comando del orquestador.
 * Todas viajan por el mismo topic (saga-replies), del que el orquestador es
 * el único consumidor.
 *
 *   sagaId            — agrupa la saga
 *   messageId         — identificador único de la réplica (idempotencia)
 *   replyToMessageId  — messageId del comando que la originó (correlación)
 *   status            — SUCCESS / FAILURE
 */
public interface SagaReply {

    String sagaId();

    String messageId();

    String replyToMessageId();

    ReplyStatus status();

    LocalDateTime respondedAt();
}
