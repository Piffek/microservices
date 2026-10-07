package com.example.order.support;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Kontenery współdzielone przez wszystkie testy integracyjne modułu (singleton container pattern).
 *
 * Kontener startuje raz na całe uruchomienie JVM i jest sprzątany przez Ryuka po jego zakończeniu.
 * Celowo NIE używamy @Container — JUnit zatrzymałby kontener po każdej klasie testowej,
 * a Spring trzyma w cache'u kontekst wskazujący na już nieistniejący port.
 *
 * Obrazy są te same co w docker-compose.yml — testujemy na tej samej wersji infrastruktury,
 * na której aplikacja działa.
 */
public final class TestContainers {

    public static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse("postgres:16-alpine");
    public static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.6.0");

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(POSTGRES_IMAGE);
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(KAFKA_IMAGE);

    private TestContainers() {
    }

    public static synchronized PostgreSQLContainer<?> postgres() {
        if (!POSTGRES.isRunning()) {
            POSTGRES.start();
        }
        return POSTGRES;
    }

    public static synchronized ConfluentKafkaContainer kafka() {
        if (!KAFKA.isRunning()) {
            KAFKA.start();
        }
        return KAFKA;
    }
}
