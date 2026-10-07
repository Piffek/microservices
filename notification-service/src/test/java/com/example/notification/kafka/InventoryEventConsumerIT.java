package com.example.notification.kafka;

import com.example.common.events.InventoryReservedEvent;
import com.example.notification.service.EmailClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Test integracyjny konsumenta "inventory.reserved" na prawdziwej Kafce.
 *
 * EmailClient jest zamockowany: to granica systemu (zewnętrzne API), a jego implementacja
 * celowo losowo zawodzi — w teście potrzebujemy determinizmu. Sprawdzamy, że event z Kafki
 * zamienia się w poprawne wywołanie wysyłki maila.
 *
 * Kontener startuje raz na JVM (singleton) — bez @Container, który zatrzymałby go po klasie,
 * psując cache kontekstu Springa.
 */
@SpringBootTest
class InventoryEventConsumerIT {

    private static final String INVENTORY_RESERVED_TOPIC = "inventory.reserved";
    private static final long VERIFY_TIMEOUT_MS = 30_000;

    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @MockBean
    private EmailClient emailClient;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void shouldSendConfirmationEmailWhenStockWasReserved() throws Exception {
        // given
        var event = InventoryReservedEvent.success(UUID.randomUUID(), "customer-1");

        // when
        publish(event);

        // then
        verify(emailClient, timeout(VERIFY_TIMEOUT_MS)).sendEmail(
                eq("customer-1@example.com"),
                eq("Order Confirmed #" + event.orderId()),
                contains(event.orderId().toString()));
    }

    @Test
    void shouldSendOrderIssueEmailWithReasonWhenReservationFailed() throws Exception {
        // given
        var event = InventoryReservedEvent.failure(UUID.randomUUID(), "customer-2", "Insufficient stock");

        // when
        publish(event);

        // then
        verify(emailClient, timeout(VERIFY_TIMEOUT_MS)).sendEmail(
                eq("customer-2@example.com"),
                eq("Order Issue #" + event.orderId()),
                contains("Reason: Insufficient stock"));
        verify(emailClient, never()).sendEmail(anyString(), eq("Order Confirmed #" + event.orderId()), anyString());
    }

    @Test
    void shouldKeepConsumingAfterMalformedMessage() throws Exception {
        // given
        var event = InventoryReservedEvent.success(UUID.randomUUID(), "customer-3");

        // when — "poison pill" na tej samej partycji co poprawny event
        kafkaTemplate.send(INVENTORY_RESERVED_TOPIC, event.orderId().toString(), "{not-a-valid-json")
                .get(10, TimeUnit.SECONDS);
        publish(event);

        // then
        verify(emailClient, timeout(VERIFY_TIMEOUT_MS)).sendEmail(
                eq("customer-3@example.com"),
                eq("Order Confirmed #" + event.orderId()),
                anyString());
    }

    private void publish(InventoryReservedEvent event) throws Exception {
        kafkaTemplate.send(INVENTORY_RESERVED_TOPIC, event.orderId().toString(), objectMapper.writeValueAsString(event))
                .get(10, TimeUnit.SECONDS);
    }

    /** Topic tworzy w produkcji order-service — tutaj zakładamy go jawnie, zamiast liczyć na auto-create brokera. */
    @TestConfiguration(proxyBeanMethods = false)
    static class KafkaTopicsConfig {

        @Bean
        NewTopic inventoryReservedTopic() {
            return TopicBuilder.name(INVENTORY_RESERVED_TOPIC).partitions(3).replicas(1).build();
        }
    }
}
