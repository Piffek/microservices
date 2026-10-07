package com.example.inventory.kafka;

import com.example.common.events.InventoryReservedEvent;
import com.example.common.events.OrderCreatedEvent;
import com.example.inventory.domain.Product;
import com.example.inventory.repository.InboxEventRepository;
import com.example.inventory.repository.ProductRepository;
import com.example.inventory.support.AbstractIntegrationTest;
import com.example.inventory.support.KafkaTestConsumer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * Testy end-to-end przez Kafkę: event na "order.created" → rezerwacja w PostgreSQL
 * → wynik na "inventory.reserved". Wejście i wyjście to wyłącznie kontrakt serwisu (topiki),
 * stan bazy sprawdzamy dodatkowo jako efekt uboczny.
 */
class OrderCreatedEventProcessingIT extends AbstractIntegrationTest {

    private static final String PRODUCT_ID = "prod-test";
    private static final String CUSTOMER_ID = "customer-1";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private InboxEventRepository inboxEventRepository;

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
    void shouldReserveStockAndPublishSuccessEvent() throws Exception {
        // given
        givenProductInStock(PRODUCT_ID, 10);
        var event = orderCreated(PRODUCT_ID, 3);

        // when
        publish(event);

        // then
        InventoryReservedEvent result = awaitSingleResultFor(event.orderId());
        assertThat(result.success()).isTrue();
        assertThat(result.orderId()).isEqualTo(event.orderId());
        assertThat(result.customerId()).isEqualTo(CUSTOMER_ID);

        assertThat(stockOf(PRODUCT_ID)).isEqualTo(7);
        assertThat(inboxEventRepository.existsByEventId(event.eventId())).isTrue();
    }

    @Test
    void shouldPublishFailureAndKeepStockWhenStockIsInsufficient() throws Exception {
        // given
        givenProductInStock(PRODUCT_ID, 2);
        var event = orderCreated(PRODUCT_ID, 5);

        // when
        publish(event);

        // then
        InventoryReservedEvent result = awaitSingleResultFor(event.orderId());
        assertThat(result.success()).isFalse();
        assertThat(result.message()).isEqualTo("Insufficient stock. Available: 2, requested: 5");

        assertThat(stockOf(PRODUCT_ID)).isEqualTo(2);
        assertThat(inboxEventRepository.existsByEventId(event.eventId())).isTrue();
    }

    @Test
    void shouldPublishFailureWhenProductDoesNotExist() throws Exception {
        // given
        var event = orderCreated("non-existing-product", 1);

        // when
        publish(event);

        // then
        InventoryReservedEvent result = awaitSingleResultFor(event.orderId());
        assertThat(result.success()).isFalse();
        assertThat(result.message()).isEqualTo("Product not found: non-existing-product");
    }

    /**
     * Inbox Pattern: ten sam eventId dostarczony dwukrotnie (at-least-once) rezerwuje towar raz.
     *
     * Żeby nie "czekać i mieć nadzieję", że duplikat nie wygeneruje wyniku, za duplikatem wysyłamy
     * event-znacznik z tym samym kluczem. Ten sam klucz = ta sama partycja = przetwarzanie po kolei,
     * więc gdy przyjdzie wynik znacznika, wynik duplikatu (gdyby powstał) musiałby już być na topicu.
     */
    @Test
    void shouldProcessDuplicatedEventOnlyOnce() throws Exception {
        // given
        givenProductInStock(PRODUCT_ID, 10);
        var event = orderCreated(PRODUCT_ID, 2);
        var marker = new OrderCreatedEvent(UUID.randomUUID(), event.orderId(), CUSTOMER_ID,
                "marker-product", 1, LocalDateTime.now());

        // when
        publish(event);
        publish(event);
        publish(marker);

        // then
        List<InventoryReservedEvent> results = awaitResultsFor(event.orderId(), 2);
        assertThat(results)
                .extracting(InventoryReservedEvent::success, InventoryReservedEvent::message)
                .containsExactly(
                        tuple(true, "Stock reserved successfully"),
                        tuple(false, "Product not found: marker-product"));

        assertThat(stockOf(PRODUCT_ID)).isEqualTo(8);
        assertThat(inboxEventRepository.count()).isEqualTo(2);
    }

    @Test
    void shouldKeepConsumingAfterMalformedMessage() throws Exception {
        // given
        givenProductInStock(PRODUCT_ID, 5);
        var event = orderCreated(PRODUCT_ID, 1);

        // when — "poison pill" na tej samej partycji co poprawny event
        kafkaTemplate.send(ORDER_CREATED_TOPIC, event.orderId().toString(), "{not-a-valid-json")
                .get(10, TimeUnit.SECONDS);
        publish(event);

        // then
        InventoryReservedEvent result = awaitSingleResultFor(event.orderId());
        assertThat(result.success()).isTrue();
        assertThat(stockOf(PRODUCT_ID)).isEqualTo(4);
    }

    private void givenProductInStock(String productId, int stock) {
        productRepository.save(Product.builder().id(productId).name("Test product").stock(stock).build());
    }

    private static OrderCreatedEvent orderCreated(String productId, int quantity) {
        return OrderCreatedEvent.of(UUID.randomUUID(), CUSTOMER_ID, productId, quantity);
    }

    private void publish(OrderCreatedEvent event) throws Exception {
        kafkaTemplate.send(ORDER_CREATED_TOPIC, event.orderId().toString(), objectMapper.writeValueAsString(event))
                .get(10, TimeUnit.SECONDS);
    }

    private InventoryReservedEvent awaitSingleResultFor(UUID orderId) {
        return awaitResultsFor(orderId, 1).getFirst();
    }

    private List<InventoryReservedEvent> awaitResultsFor(UUID orderId, int expectedCount) {
        return inventoryReservedConsumer.awaitRecords(record -> orderId.toString().equals(record.key()), expectedCount)
                .stream()
                .map(ConsumerRecord::value)
                .map(this::toInventoryReservedEvent)
                .toList();
    }

    private InventoryReservedEvent toInventoryReservedEvent(String json) {
        try {
            return objectMapper.readValue(json, InventoryReservedEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize InventoryReservedEvent: " + json, e);
        }
    }

    private int stockOf(String productId) {
        return productRepository.findById(productId).orElseThrow().getStock();
    }
}
