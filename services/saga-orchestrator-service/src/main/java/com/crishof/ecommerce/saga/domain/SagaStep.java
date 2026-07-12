package com.crishof.ecommerce.saga.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * Un paso de la saga: el comando enviado y (cuando llega) su réplica.
 * Permite reconstruir la historia completa con un SELECT.
 */
@Entity
@Table(name = "saga_steps")
public class SagaStep {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "saga_id", nullable = false, length = 64)
    private String sagaId;

    @Column(name = "step_number", nullable = false)
    private Integer stepNumber;

    @Column(name = "step_type", nullable = false, length = 50)
    private String stepType;

    @Column(name = "message_id", nullable = false, length = 64)
    private String messageId;

    @Column(name = "command_type", nullable = false, length = 80)
    private String commandType;

    @Column(name = "command_json", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String commandJson;

    @Column(name = "reply_status", length = 20)
    private String replyStatus;

    @Column(name = "reply_json", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String replyJson;

    @Column(name = "dispatched_at", nullable = false)
    private LocalDateTime dispatchedAt;

    @Column(name = "replied_at")
    private LocalDateTime repliedAt;

    public SagaStep() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getSagaId() {
        return sagaId;
    }

    public void setSagaId(String sagaId) {
        this.sagaId = sagaId;
    }

    public Integer getStepNumber() {
        return stepNumber;
    }

    public void setStepNumber(Integer stepNumber) {
        this.stepNumber = stepNumber;
    }

    public String getStepType() {
        return stepType;
    }

    public void setStepType(String stepType) {
        this.stepType = stepType;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getCommandType() {
        return commandType;
    }

    public void setCommandType(String commandType) {
        this.commandType = commandType;
    }

    public String getCommandJson() {
        return commandJson;
    }

    public void setCommandJson(String commandJson) {
        this.commandJson = commandJson;
    }

    public String getReplyStatus() {
        return replyStatus;
    }

    public void setReplyStatus(String replyStatus) {
        this.replyStatus = replyStatus;
    }

    public String getReplyJson() {
        return replyJson;
    }

    public void setReplyJson(String replyJson) {
        this.replyJson = replyJson;
    }

    public LocalDateTime getDispatchedAt() {
        return dispatchedAt;
    }

    public void setDispatchedAt(LocalDateTime dispatchedAt) {
        this.dispatchedAt = dispatchedAt;
    }

    public LocalDateTime getRepliedAt() {
        return repliedAt;
    }

    public void setRepliedAt(LocalDateTime repliedAt) {
        this.repliedAt = repliedAt;
    }
}
