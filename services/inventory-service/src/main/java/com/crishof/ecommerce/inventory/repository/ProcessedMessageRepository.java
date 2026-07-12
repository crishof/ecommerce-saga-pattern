package com.crishof.ecommerce.inventory.repository;

import com.crishof.ecommerce.inventory.domain.ProcessedMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, String> {
}
