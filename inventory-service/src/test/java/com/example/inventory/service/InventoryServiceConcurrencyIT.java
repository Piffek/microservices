package com.example.inventory.service;

import com.example.common.events.InventoryReservedEvent;
import com.example.common.events.OrderCreatedEvent;
import com.example.inventory.domain.Product;
import com.example.inventory.repository.InboxEventRepository;
import com.example.inventory.repository.ProductRepository;
import com.example.inventory.support.AbstractIntegrationTest;
import com.example.inventory.support.KafkaTestConsumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Weryfikuje blokadę pesymistyczną (SELECT ... FOR UPDATE) przy równoległych rezerwacjach.
 * Bez niej transakcje czytałyby ten sam stan magazynu i nadpisywały sobie wynik (lost update),
 * czyli sprzedalibyśmy więcej towaru, niż mamy. Taki błąd ujawnia się tylko na prawdziwej bazie.
 */
class InventoryServiceConcurrencyIT extends AbstractIntegrationTest {

    private static final String PRODUCT_ID = "prod-limited";
    private static final int AVAILABLE_STOCK = 5;
    private static final int CONCURRENT_ORDERS = 20;

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private InboxEventRepository inboxEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaTestConsumer inventoryReservedConsumer;

    @BeforeEach
    void setUp() {
        inboxEventRepository.deleteAllInBatch();
        productRepository.deleteAllInBatch();
        inventoryReservedConsumer = KafkaTestConsumer.subscribe(kafkaBootstrapServers(), INVENTORY_RESERVED_TOPIC);
    }

    @AfterEach
    void tearDown() {
        inventoryReservedConsumer.close();
    }

    @Test
    void shouldNeverOversellWhenOrdersCompeteForLastItems() throws Exception {
        // given
        productRepository.save(Product.builder().id(PRODUCT_ID).name("Limited edition").stock(AVAILABLE_STOCK).build());
        List<OrderCreatedEvent> events = IntStream.range(0, CONCURRENT_ORDERS)
                .mapToObj(i -> OrderCreatedEvent.of(UUID.randomUUID(), "customer-" + i, PRODUCT_ID, 1))
                .toList();

        // when
        processConcurrently(events);

        // then
        assertThat(productRepository.findById(PRODUCT_ID).orElseThrow().getStock()).isZero();
        assertThat(inboxEventRepository.count()).isEqualTo(CONCURRENT_ORDERS);

        Set<String> orderIds = events.stream().map(e -> e.orderId().toString()).collect(Collectors.toSet());
        List<ConsumerRecord<String, String>> records =
                inventoryReservedConsumer.awaitRecords(record -> orderIds.contains(record.key()), CONCURRENT_ORDERS);

        long successfulReservations = records.stream()
                .map(record -> readEvent(record.value()))
                .filter(InventoryReservedEvent::success)
                .count();
        assertThat(successfulReservations).isEqualTo(AVAILABLE_STOCK);
    }

    /** Bramka startowa (latch) puszcza wszystkie wątki naraz, żeby maksymalnie zwiększyć rywalizację o blokadę. */
    private void processConcurrently(List<OrderCreatedEvent> events) throws Exception {
        var startGate = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newFixedThreadPool(events.size())) {
            CompletableFuture<?>[] tasks = events.stream()
                    .map(event -> CompletableFuture.runAsync(() -> {
                        awaitQuietly(startGate);
                        inventoryService.processOrderCreated(event);
                    }, executor))
                    .toArray(CompletableFuture[]::new);

            startGate.countDown();
            CompletableFuture.allOf(tasks).get(30, TimeUnit.SECONDS);
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private InventoryReservedEvent readEvent(String json) {
        try {
            return objectMapper.readValue(json, InventoryReservedEvent.class);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot deserialize InventoryReservedEvent: " + json, e);
        }
    }
}
