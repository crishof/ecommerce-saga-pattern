# E-commerce · Saga Pattern con Orchestration

![Java](https://img.shields.io/badge/Java-25-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.0-brightgreen)
![Spring Cloud](https://img.shields.io/badge/Spring%20Cloud-2025.1.2-blue)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-black)

Séptimo proyecto de la serie de arquitecturas. Reimplementa la saga distribuida
del **proyecto 19** (`ecommerce-microservices`) pero cambiando el patrón de
coordinación:

- **Proyecto 19 — CHOREOGRAPHY**: cada servicio reacciona a los eventos que otros
  publican. No hay coordinador; el flujo global es implícito.
- **Proyecto 20 — ORCHESTRATION**: un servicio dedicado (`saga-orchestrator`)
  dirige el flujo enviando **comandos** y avanzando su máquina de estados con las
  **réplicas** que recibe.

La misma infraestructura (Spring Cloud Netflix, Kafka, outbox) sirve de base para
comparar los dos patrones _side-by-side_. Los puertos son distintos a los del 19,
de modo que ambos stacks pueden correr en paralelo.

---

## Choreography vs Orchestration

```
CHOREOGRAPHY (proyecto 19)                 ORCHESTRATION (proyecto 20)
──────────────────────────                 ───────────────────────────
order  ─OrderPlaced─▶ (bus)                order ─OrderPlaced─▶ orchestrator
   inventory ⭢ StockReserved                  │  crea saga (RESERVING_STOCK)
      payment ⭢ PaymentSucceeded              └─ ReserveStockCommand ─▶ inventory
         order ⭢ PAID                         inventory ─StockReservationReply─▶ orch
                                                 └─ ChargePaymentCommand ─▶ payment
Cada servicio decide su siguiente paso      payment ─PaymentReply─▶ orch
reaccionando a eventos. Nadie conoce           └─ ConfirmOrderCommand ─▶ order
el flujo completo.                             └─ SendNotificationCommand ─▶ notif
                                            El orquestador conoce y dirige TODO
                                            el flujo; su BD cuenta la historia.
```

---

## La pieza nueva: `saga-orchestrator-service`

Servicio dedicado (puerto **8087**, BD `saga_db`) con:

- **Máquina de estados formal** (`SagaState`, 11 estados) — `SagaStateMachine`
  decide qué comando enviar según el estado actual y el resultado de cada réplica.
- **Tabla `saga_instances`** — el estado de cada saga en una sola fila.
- **Tabla `saga_steps`** — histórico de cada paso con el comando y su réplica en JSON.
- **Comandos vs eventos** — envía comandos dirigidos, recibe réplicas.
- **Timeouts** — un `@Scheduled` cada 5s compensa las sagas que superan 30s
  (resiliencia frente a servicios caídos).
- **Idempotencia** — `processed_messages` en el orquestador y en cada servicio que
  consume comandos; cada mensaje lleva un `messageId` único.
- **Outbox pattern** — los comandos se publican vía outbox, de modo que el
  orquestador sobrevive a caídas.
- **Endpoints REST de introspección** — el diferencial clave frente al 19.

### Máquina de estados

```
STARTED
   ↓
RESERVING_STOCK      ── ReserveStockCommand           ─▶ inventory
   ↓ StockReservationReply(SUCCESS)
CHARGING_PAYMENT     ── ChargePaymentCommand           ─▶ payment
   ↓ PaymentReply(SUCCESS)
CONFIRMING_ORDER     ── ConfirmOrderCommand            ─▶ order
   ↓ OrderStatusUpdateReply(PAID)
NOTIFYING_SUCCESS    ── SendOrderConfirmedNotification ─▶ notification
   ↓ NotificationReply(SUCCESS)
COMPLETED  ✅

Compensación (LIFO), p. ej. si falla el pago:
CHARGING_PAYMENT ─PaymentReply(FAILURE)─▶ COMPENSATING_PAYMENT (RefundPayment)
   ─▶ COMPENSATING_STOCK (ReleaseStock) ─▶ CANCELLING_ORDER (CancelOrder)
   ─▶ NOTIFYING_FAILURE (SendOrderCancelledNotification) ─▶ FAILED  ❌
```

---

## Endpoints nuevos (vs el 19)

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/api/sagas` | Lista de sagas con paginación y filtro por estado (`?state=FAILED`) |
| `GET` | `/api/sagas/{sagaId}` | Detalle de una saga con el histórico completo de pasos |
| `GET` | `/api/sagas/stats` | Conteo por estado (útil para dashboards) |

Los casos de uso de negocio son idénticos al roadmap:

- **CASO 1** — `POST /api/catalog/products`
- **CASO 2** — `POST /api/orders` (saga distribuida con compensación)
- **CASO 3** — `GET /api/customers/{id}/orders`

---

## Topics de Kafka

| Estilo | Topic | Emisor → Consumidor |
|--------|-------|---------------------|
| Evento | `order-events` | order → orchestrator (dispara la saga) + audit trail |
| Comando | `inventory-commands` | orchestrator → inventory |
| Comando | `payment-commands` | orchestrator → payment |
| Comando | `order-commands` | orchestrator → order |
| Comando | `notification-commands` | orchestrator → notification |
| Réplica | `saga-replies` | todos los servicios → orchestrator |

Headers: `x-saga-id`, `x-message-id`, `x-message-type`, `x-correlation-id`,
`x-source-service` (`SagaHeaders`).

---

## Comparación choreography (19) vs orchestration (20)

| Aspecto | 19 (choreography) | 20 (orchestration) |
|---------|--------------------|---------------------|
| Coordinador | No hay | `saga-orchestrator` |
| Estado de la saga | Distribuido en cada servicio | Centralizado en `saga_instances` |
| Trazabilidad | Reconstruir de logs | Query SQL / REST |
| Acoplamiento | Bajo (eventos) | Medio (comandos con destino) |
| Debugging | Complicado (`tail -f` a 6 logs) | Trivial (una fila cuenta la historia) |
| Cambiar un paso | Modificar varios servicios | Modificar el orquestador |
| Punto único de fallo | No | Sí (el orquestador, mitigado con outbox) |
| Complejidad de código | Menor | Mayor pero más explícito |
| Monto del cobro | Proyección local en payment | Viaja en `ChargePaymentCommand` |

---

## Cómo ejecutar

```bash
# Asegurar que el proyecto 19 NO está corriendo (comparten el broker Kafka
# interno en 9092). Los puertos EXTERNOS son distintos, pero por seguridad:
docker ps | grep ecommerce-ms   # debe estar vacío

./mvnw clean package -DskipTests
docker compose up -d
sleep 120

# Verificar registros en Eureka
curl http://localhost:8762/eureka/apps -H "Accept: application/json"

# CASO 1 — crear producto
PROD=$(curl -s -X POST http://localhost:8080/api/catalog/products \
  -H "Content-Type: application/json" \
  -d '{"sku":"SAGA-001","name":"Laptop","description":"desc","price":1299.99,"stock":10}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

# CASO 2 — crear pedido (dispara la saga)
ORDER=$(curl -s -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d "{\"customerId\":2,\"lines\":[{\"productId\":$PROD,\"quantity\":2}]}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['orderId'])")

sleep 5

# CASO 3 — historial del cliente
curl -s "http://localhost:8080/api/customers/2/orders" | python3 -m json.tool

# ¡NOVEDAD DEL 20! Consultar la saga en el orquestador
curl -s "http://localhost:8080/api/sagas" | python3 -m json.tool

# Detalle con TODOS los pasos (comando + réplica de cada uno)
SAGA_ID=$(curl -s "http://localhost:8080/api/sagas" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['content'][0]['sagaId'])")
curl -s "http://localhost:8080/api/sagas/$SAGA_ID" | python3 -m json.tool

# Estadísticas por estado
curl -s "http://localhost:8080/api/sagas/stats" | python3 -m json.tool

docker compose down -v
```

---

## Puertos

| Servicio | Puerto | PostgreSQL (externo) |
|----------|--------|----------------------|
| api-gateway | 8080 | — |
| identity-service | 8081 | 5461 |
| catalog-service | 8082 | 5462 |
| order-service | 8083 | 5463 |
| payment-service | 8084 | 5464 |
| inventory-service | 8085 | 5465 |
| notification-service | 8086 | 5466 |
| **saga-orchestrator-service** | **8087** | **5467** |
| discovery-server (Eureka) | 8762 | — |
| config-server | 8889 | — |
| kafka | 9096 (ext) / 9092 (int) | — |
| kafka-ui | 8091 | — |

> Todos los puertos son distintos a los del proyecto 19 para poder correr los dos
> stacks en paralelo durante la comparación.

---

## Idempotencia

Comandos y réplicas pueden llegar duplicados (retry de Kafka). Cada mensaje lleva
un `messageId` único (UUID) y cada servicio guarda una tabla `processed_messages`
con los IDs vistos: antes de procesar comprueba si el mensaje ya está registrado.
Además, cada operación se asocia al `sagaId` (por ejemplo, `inventory` guarda
`saga_id` en sus reservas) para que un retry del mismo comando no duplique la
operación de negocio.

---

## Limitaciones y qué resolverá el proyecto 21

- Sin observabilidad end-to-end (métricas Prometheus, tracing Jaeger).
- Sin dashboard visual de las sagas → el **proyecto 21** (`ecommerce-observability`)
  añadirá Grafana sobre esta misma base.

---

## Portfolio

| # | Proyecto | Descripción |
|---|----------|-------------|
| 01 | [java-oop-fundamentals](https://github.com/crishof/java-oop-fundamentals) | POO, generics, records, sealed classes, patrones |
| 02 | [java-collections-streams](https://github.com/crishof/java-collections-streams) | Collections Framework y Streams API |
| 03 | [spring-core-ioc](https://github.com/crishof/spring-core-ioc) | Spring Core, IoC, DI y AOP |
| 04 | [spring-rest-api](https://github.com/crishof/spring-rest-api) | REST API con Spring MVC y OpenAPI |
| 05 | [spring-jpa-hibernate](https://github.com/crishof/spring-jpa-hibernate) | JPA, Hibernate, relaciones y caché |
| 06 | [spring-data-jpa](https://github.com/crishof/spring-data-jpa) | Spring Data JPA, paginación y specs |
| 07 | [spring-security-jwt](https://github.com/crishof/spring-security-jwt) | Spring Security, JWT y OAuth2 |
| 08 | [spring-testing](https://github.com/crishof/spring-testing) | Testing profesional con JUnit y Testcontainers |
| 09 | [spring-async](https://github.com/crishof/spring-async) | @Async, CompletableFuture y Scheduling |
| 10 | [spring-rabbitmq](https://github.com/crishof/spring-rabbitmq) | Mensajería con RabbitMQ y AMQP |
| 11 | [spring-kafka](https://github.com/crishof/spring-kafka) | Event streaming con Apache Kafka |
| 12 | [spring-docker](https://github.com/crishof/spring-docker) | Containerización con Docker y Compose |
| 13 | [spring-cicd](https://github.com/crishof/spring-cicd) | CI/CD con GitHub Actions |
| 14 | [ecommerce-layered-architecture](https://github.com/crishof/ecommerce-layered-architecture) | Monolito en capas (N-Tier) — BASELINE |
| 15 | [ecommerce-modular-monolith](https://github.com/crishof/ecommerce-modular-monolith) | Monolito modular por dominios |
| 16 | [ecommerce-hexagonal](https://github.com/crishof/ecommerce-hexagonal) | Arquitectura hexagonal (Ports & Adapters) |
| 17 | [ecommerce-clean-architecture](https://github.com/crishof/ecommerce-clean-architecture) | Clean Architecture (Uncle Bob) |
| 18 | [ecommerce-cqrs-event-sourcing](https://github.com/crishof/ecommerce-cqrs-event-sourcing) | CQRS y Event Sourcing |
| 19 | [ecommerce-microservices](https://github.com/crishof/ecommerce-microservices) | Microservicios con Spring Cloud (choreography) |
| 20 | [ecommerce-saga-pattern](https://github.com/crishof/ecommerce-saga-pattern) ← *este proyecto* | Saga Pattern con orchestration |
| 21 | [ecommerce-observability](https://github.com/crishof/ecommerce-observability) | Observabilidad con Prometheus y Grafana |

---
_Autor: **Cristian Hoffmann** — Proyecto académico Java 25 / Spring Boot 4.1.0._
