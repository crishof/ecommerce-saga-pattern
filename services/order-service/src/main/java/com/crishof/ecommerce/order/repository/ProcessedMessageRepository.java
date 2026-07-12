package com.crishof.ecommerce.order.repository;

import com.crishof.ecommerce.order.domain.ProcessedMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
}
