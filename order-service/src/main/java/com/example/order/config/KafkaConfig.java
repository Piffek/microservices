package com.example.order.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Konfiguracja Kafki — tworzenie topicków przy starcie aplikacji.
 *
 * TOPICI (tematy) w Kafce:
 * - "order.created"       → Order Service → Inventory Service
 * - "inventory.reserved"  → Inventory Service → Notification Service
 *
 * Parametry topica:
 * - partitions(3): 3 partycje = możliwość równoległego przetwarzania przez 3 konsumentów
 * - replicas(1): replikacja = 1 (na potrzeby dev; w prod min. 3 dla HA)
 *
 * PARTYCJONOWANIE w Kafce:
 * Każda wiadomość trafia do jednej partycji. Kolejność wiadomości jest
 * gwarantowana TYLKO w obrębie jednej partycji.
 * Wysyłamy z kluczem = orderId, więc wszystkie eventy jednego zamówienia
 * trafiają do tej samej partycji (zachowana kolejność dla danego zamówienia).
 */
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder.name("order.created")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic inventoryReservedTopic() {
        return TopicBuilder.name("inventory.reserved")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
