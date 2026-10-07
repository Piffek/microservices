package com.example.order.kafka;

import com.example.common.events.OrderCreatedEvent;
import com.example.order.controller.dto.CreateOrderRequest;
import com.example.order.domain.Order;
import com.example.order.domain.OutboxEvent;
import com.example.order.repository.OrderRepository;
import com.example.order.repository.OutboxEventRepository;
import com.example.order.service.OrderService;
import com.example.order.support.AbstractIntegrationTest;
import com.example.order.support.KafkaTestConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Weryfikuje Outbox Pattern end-to-end: zapis zamówienia → OutboxScheduler → topic "order.created".
 * Scheduler działa naprawdę (nie wołamy go ręcznie) — sprawdzamy zachowanie, a nie implementację.
 */
class OutboxPublishingIT extends AbstractIntegrationTest {

    private static final String ORDER_CREATED_TOPIC = "order.created";

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaTestConsumer orderCreatedConsumer;

    @BeforeEach
    void setUp() {
        outboxEventRepository.deleteAllInBatch();
        orderRepository.deleteAllInBatch();
        orderCreatedConsumer = KafkaTestConsumer.subscribe(kafkaBootstrapServers(), ORDER_CREATED_TOPIC);
    }

    @AfterEach
    void tearDown() {
        orderCreatedConsumer.close();
    }

    @Test
    void shouldPublishOrderCreatedEventToKafka() throws Exception {
        // given
        Order order = orderService.createOrder(new CreateOrderRequest("customer-1", "prod-456", 3, 10.00));

        // when
        ConsumerRecord<String, String> record = orderCreatedConsumer.awaitSingleRecordWithKey(order.getId().toString());

        // then
        OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);
        assertThat(event.orderId()).isEqualTo(order.getId());
        assertThat(event.customerId()).isEqualTo("customer-1");
        assertThat(event.productId()).isEqualTo("prod-456");
        assertThat(event.quantity()).isEqualTo(3);
    }

    @Test
    void shouldMarkOutboxEventAsPublishedAfterBrokerAcknowledgement() {
        // given
        Order order = orderService.createOrder(new CreateOrderRequest("customer-2", "prod-123", 1, 99.00));
        OutboxEvent outboxEvent = outboxEventRepository.findAll().stream()
                .filter(event -> event.getAggregateId().equals(order.getId()))
                .findFirst()
                .orElseThrow();

        // when
        orderCreatedConsumer.awaitSingleRecordWithKey(order.getId().toString());

        // then
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(outboxEventRepository.findById(outboxEvent.getId())).hasValueSatisfying(published -> {
                    assertThat(published.isPublished()).isTrue();
                    assertThat(published.getPublishedAt()).isNotNull();
                }));
    }
}
