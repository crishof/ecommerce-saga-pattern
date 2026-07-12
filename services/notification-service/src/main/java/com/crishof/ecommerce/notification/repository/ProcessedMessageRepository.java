package com.crishof.ecommerce.notification.repository;

import com.crishof.ecommerce.notification.domain.ProcessedMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
}
