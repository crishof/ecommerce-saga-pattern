package com.crishof.ecommerce.saga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * saga-orchestrator-service: coordinador central de la saga distribuida.
 * Dirige el flujo con comandos y avanza su máquina de estados con las réplicas.
 */
@SpringBootApplication
@EnableScheduling      // outbox publisher + timeout scheduler
public class SagaOrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SagaOrchestratorApplication.class, args);
    }
}
