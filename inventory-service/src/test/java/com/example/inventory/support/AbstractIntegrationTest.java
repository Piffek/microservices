package com.example.inventory.support;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

/**
 * Bazowa klasa testów integracyjnych Inventory Service — pełny kontekst Springa
 * na prawdziwym PostgreSQL i Kafce.
 *
 * WAŻNE: klasy dziedziczące nie mogą zmieniać konfiguracji kontekstu (@MockBean itp.).
 * Drugi kontekst = drugi zestaw @KafkaListenerów w tej samej consumer group
 * (rebalance, "kradzież" wiadomości między kontekstami) oraz create-drop schematu
 * pod działającym pierwszym kontekstem.
 */
@SpringBootTest
@Import(AbstractIntegrationTest.KafkaTopicsConfig.class)
public abstract class AbstractIntegrationTest {

    protected static final String ORDER_CREATED_TOPIC = "order.created";
    protected static final String INVENTORY_RESERVED_TOPIC = "inventory.reserved";

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = TestContainers.postgres();

    static final ConfluentKafkaContainer KAFKA = TestContainers.kafka();

    /**
     * KafkaConsumerConfig czyta adres brokera przez @Value("${spring.kafka.bootstrap-servers}"),
     * więc ustawiamy właściwość wprost — @ServiceConnection dla Kafki by tu nie wystarczył.
     */
    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    protected static String kafkaBootstrapServers() {
        return KAFKA.getBootstrapServers();
    }

    /**
     * Topiki tworzy w produkcji order-service. W teście inventory-service działa sam,
     * więc tworzymy je jawnie zamiast polegać na auto.create.topics.enable brokera.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class KafkaTopicsConfig {

        @Bean
        NewTopic orderCreatedTopic() {
            return TopicBuilder.name(ORDER_CREATED_TOPIC).partitions(3).replicas(1).build();
        }

        @Bean
        NewTopic inventoryReservedTopic() {
            return TopicBuilder.name(INVENTORY_RESERVED_TOPIC).partitions(3).replicas(1).build();
        }
    }
}
