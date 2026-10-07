package com.example.order.support;

import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

/**
 * Bazowa klasa testów integracyjnych Order Service — pełny kontekst Springa
 * na prawdziwym PostgreSQL i Kafce.
 *
 * WAŻNE: wszystkie klasy dziedziczące muszą mieć IDENTYCZNĄ konfigurację kontekstu
 * (bez @MockBean, @TestPropertySource itp.). Wtedy Spring buduje kontekst raz i reużywa go
 * z cache'a. Drugi kontekst oznaczałby drugi OutboxScheduler działający na tej samej bazie
 * oraz ponowne create-drop schematu w trakcie działania pierwszego — źródło flaky testów.
 */
@SpringBootTest
@AutoConfigureMockMvc
public abstract class AbstractIntegrationTest {

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = TestContainers.postgres();

    static final ConfluentKafkaContainer KAFKA = TestContainers.kafka();

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    protected static String kafkaBootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}