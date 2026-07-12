package com.crishof.ecommerce.payment.repository;

import com.crishof.ecommerce.payment.domain.ProcessedMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
}
