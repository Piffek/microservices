ckaude# Microservices Learning Project

Projekt edukacyjny demonstrujący popularne wzorce mikroserwisowe:
**Kafka · Outbox Pattern · Inbox Pattern · Circuit Breaker · Event-Driven Architecture**

## Architektura

```
┌─────────────┐     HTTP POST      ┌───────────────────────────────────────┐
│   Klient    │ ──────────────────► │           ORDER SERVICE               │
│ (curl/http) │                    │  port: 8081                           │
└─────────────┘                    │                                       │
                                   │  1. Zapisuje Order do DB              │
                                   │  2. Zapisuje OutboxEvent do DB        │
                                   │     (W JEDNEJ TRANSAKCJI!)            │
                                   │  3. Scheduler → wysyła do Kafki       │
                                   └───────────────┬───────────────────────┘
                                                   │
                                      topic: order.created
                                                   │
                                   ┌───────────────▼───────────────────────┐
                                   │         INVENTORY SERVICE             │
                                   │  port: 8082                           │
                                   │                                       │
                                   │  1. Sprawdza Inbox (idempotentność)   │
                                   │  2. Rezerwuje towar w DB              │
                                   │  3. Zapisuje do Inbox                 │
                                   │     (W JEDNEJ TRANSAKCJI!)            │
                                   │  4. Emituje event z wynikiem          │
                                   └───────────────┬───────────────────────┘
                                                   │
                                    topic: inventory.reserved
                                                   │
                                   ┌───────────────▼───────────────────────┐
                                   │        NOTIFICATION SERVICE           │
                                   │  port: 8083                           │
                                   │                                       │
                                   │  Circuit Breaker ──► Email API        │
                                   │  (Resilience4j)    (symulowany)       │
                                   └───────────────────────────────────────┘

Infrastruktura:
  PostgreSQL :5432  — bazy: orderdb, inventorydb
  Kafka      :9092  — topiki: order.created, inventory.reserved
  Kafka UI   :8090  — http://localhost:8090 (podgląd w przeglądarce)
  Prometheus :9090  — http://localhost:9090 (źródło metryk)
  Grafana    :3000  — http://localhost:3000 (dashboardy, admin/admin)
```

## Monitoring

Każda mikrousługa udostępnia metryki Prometheus pod adresem `/actuator/prometheus`.
Prometheus zbiera je automatycznie, a Grafana korzysta z gotowego źródła danych
`Prometheus` skonfigurowanego przez Docker Compose.

Uruchomienie całego środowiska:

```bash
docker compose up --build
```

Po uruchomieniu otwórz `http://localhost:3000` i zaloguj się danymi `admin/admin`.

## Wzorce mikroserwisowe w projekcie

### 1. Outbox Pattern (Order Service)

**Problem:** Nie możesz atomowo zapisać do DB i wysłać do Kafki.
Jeśli Kafka jest niedostępna po zapisie do DB → event przepada.
Jeśli DB rollbackuje po wysłaniu do Kafki → ghost event.

**Rozwiązanie:**
```
Transakcja DB:
  ├── INSERT INTO orders (...)
  └── INSERT INTO outbox_events (payload, published=false)  ← atomowo!

Scheduler (co 2s):
  ├── SELECT * FROM outbox_events WHERE published=false FOR UPDATE SKIP LOCKED
  ├── kafkaTemplate.send(topic, payload).get()  ← wyślij do Kafki
  └── UPDATE outbox_events SET published=true   ← oznacz po sukcesie
```

**Pliki:**
- `OutboxEvent.java` — encja tabeli outbox
- `OrderService.java` — zapis Order + OutboxEvent w jednej transakcji
- `OutboxScheduler.java` — odczyt i wysyłka do Kafki
- `OutboxEventRepository.java` — query z FOR UPDATE SKIP LOCKED

---

### 2. Inbox Pattern (Inventory Service)

**Problem:** Kafka gwarantuje at-least-once delivery — event może dotrzeć wielokrotnie.
Bez ochrony → duplikaty, podwójne rezerwacje, zduplikowane emaile.

**Rozwiązanie:**
```
Konsument odbiera event:
  ├── IF EXISTS(SELECT 1 FROM inbox_events WHERE event_id = ?) → SKIP
  └── Transakcja DB:
        ├── [logika biznesowa] UPDATE products SET stock = stock - qty
        └── INSERT INTO inbox_events (event_id, ...)  ← atomowo!
```

**Pliki:**
- `InboxEvent.java` — encja tabeli inbox (PK = eventId z Kafki)
- `InventoryService.java` — sprawdzenie inbox + logika + zapis inbox
- `InboxEventRepository.java` — existsByEventId()

---

### 3. Circuit Breaker (Notification Service)

**Problem:** Zewnętrzne API emailowe może być niestabilne.
Bez zabezpieczenia: każde wywołanie czeka na timeout (np. 30s) → thread starvation.

**Stany Circuit Breakera:**
```
CLOSED → (>50% błędów z 10 ostatnich) → OPEN → (po 10s) → HALF_OPEN → (3 sukcesy) → CLOSED
                                           ↑                    ↓
                                           └──── (błąd) ────────┘
```

**Konfiguracja (application.yml):**
```yaml
resilience4j.circuitbreaker.instances.emailService:
  sliding-window-size: 10          # analizuj ostatnie 10 wywołań
  failure-rate-threshold: 50       # otwórz CB gdy >= 50% błędów
  wait-duration-in-open-state: 10s # 10s w stanie OPEN
  permitted-number-of-calls-in-half-open-state: 3  # 3 testy
```

**Pliki:**
- `EmailClient.java` — @CircuitBreaker + fallback method
- `application.yml` — konfiguracja progów CB

---

### 4. Event-Driven Architecture

Serwisy komunikują się **wyłącznie przez eventy na Kafce** — zero bezpośrednich wywołań HTTP.

**Korzyści:**
- **Loose coupling** — Order Service nie wie, że Inventory Service istnieje
- **Resilience** — Inventory Service może być niedostępny; eventy zaczekają w Kafce
- **Scalability** — każdy serwis skaluje się niezależnie
- **Extensibility** — nowy serwis (np. Analytics) subskrybuje topic bez modyfikacji istniejących serwisów

**Topiki:**
```
order.created        → emituje Order Service, konsumuje Inventory Service
inventory.reserved   → emituje Inventory Service, konsumuje Notification Service
```

---

## Uruchomienie

### Docker (zalecane)

```bash
# Zbuduj i uruchom wszystko
docker-compose up --build

# Sprawdź logi konkretnego serwisu
docker-compose logs -f order-service
docker-compose logs -f inventory-service
docker-compose logs -f notification-service

# Zatrzymaj i usuń dane
docker-compose down -v
```

### Złóż zamówienie (testowe)

```bash
# Zamówienie z dostępnym towarem
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"user-1","productId":"prod-123","quantity":2,"pricePerUnit":499.99}'

# Zamówienie bez towaru (prod-789 ma stock=0)
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"user-2","productId":"prod-789","quantity":1,"pricePerUnit":29.99}'

# Nieistniejący produkt
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"user-3","productId":"prod-UNKNOWN","quantity":1,"pricePerUnit":9.99}'
```

### Obserwuj eventy

Otwórz **http://localhost:8090** (Kafka UI) i obserwuj wiadomości na topikach.

---

## Struktura projektu

```
microservices/
├── common/                          # Współdzielone klasy eventów
│   └── src/main/java/com/example/common/events/
│       ├── OrderCreatedEvent.java
│       └── InventoryReservedEvent.java
│
├── order-service/                   # Serwis zamówień (port 8081)
│   └── src/main/java/com/example/order/
│       ├── domain/
│       │   ├── Order.java           # Encja zamówienia
│       │   └── OutboxEvent.java     # ★ Outbox Pattern
│       ├── service/
│       │   └── OrderService.java    # ★ Atomowy zapis Order + OutboxEvent
│       ├── kafka/
│       │   └── OutboxScheduler.java # ★ Wysyłka do Kafki
│       └── controller/
│           └── OrderController.java # REST API
│
├── inventory-service/               # Serwis magazynu (port 8082)
│   └── src/main/java/com/example/inventory/
│       ├── domain/
│       │   ├── Product.java         # Encja produktu
│       │   └── InboxEvent.java      # ★ Inbox Pattern
│       ├── service/
│       │   └── InventoryService.java# ★ Idempotentne przetwarzanie
│       └── kafka/
│           └── OrderEventConsumer.java # Konsument z manual ack
│
├── notification-service/            # Serwis powiadomień (port 8083)
│   └── src/main/java/com/example/notification/
│       ├── service/
│       │   ├── NotificationService.java
│       │   └── EmailClient.java     # ★ Circuit Breaker
│       └── kafka/
│           └── InventoryEventConsumer.java
│
├── docker-compose.yml               # Cała infrastruktura
└── init-db.sql                      # Tworzenie baz danych
```

---

## Dostępne produkty (preloaded)

| ID         | Nazwa               | Ilość na stanie |
|------------|---------------------|-----------------|
| prod-123   | Laptop Pro 15       | 10              |
| prod-456   | Mechanical Keyboard | 50              |
| prod-789   | USB-C Hub           | 0 (brak!)       |

---

## Ćwiczenia do samodzielnego wykonania

1. **Aktualizacja statusu zamówienia** — dodaj konsumenta w Order Service,
   który nasłuchuje na `inventory.reserved` i aktualizuje status zamówienia
   z `PENDING` na `CONFIRMED` lub `CANCELLED`.

2. **Dead Letter Topic** — skonfiguruj Spring Kafka `DeadLetterPublishingRecoverer`,
   który po 3 nieudanych próbach przetworzenia wysyła event na topic `*.DLT`.

3. **Saga Pattern** — rozszerz Inventory Service: jeśli rezerwacja się nie uda,
   wyemituj event kompensacyjny `OrderCancelledEvent`, który Order Service przetworzy.

4. **Retry z Resilience4j** — dodaj `@Retry(name = "emailService")` do EmailClient,
   żeby próbował wysłać email N razy zanim uruchomi fallback.

5. **Flyway** — zastąp `ddl-auto: create-drop` migracjami Flyway (`V1__init.sql`).

6. **Skalowanie** — uruchom 2 instancje Inventory Service i sprawdź w Kafka UI
   jak partycje są rozdzielane między konsumentów.

---

## Monitorowanie Circuit Breakera

```bash
# Stan Circuit Breakera (wymaga uruchomionego notification-service)
curl http://localhost:8083/actuator/health | jq .

# Metryki CB
curl http://localhost:8083/actuator/metrics/resilience4j.circuitbreaker.state
```
