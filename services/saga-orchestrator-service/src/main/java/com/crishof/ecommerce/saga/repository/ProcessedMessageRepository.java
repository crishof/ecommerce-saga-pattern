package com.crishof.ecommerce.saga.repository;

import com.crishof.ecommerce.saga.domain.ProcessedMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
}
