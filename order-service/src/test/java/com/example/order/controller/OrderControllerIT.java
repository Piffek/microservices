package com.example.order.controller;

import com.example.common.events.OrderCreatedEvent;
import com.example.order.controller.dto.CreateOrderRequest;
import com.example.order.domain.OrderStatus;
import com.example.order.repository.OrderRepository;
import com.example.order.repository.OutboxEventRepository;
import com.example.order.support.AbstractIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class OrderControllerIT extends AbstractIntegrationTest {

    private static final String CUSTOMER_ID = "customer-1";
    private static final String PRODUCT_ID = "prod-123";

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void cleanDatabase() {
        outboxEventRepository.deleteAllInBatch();
        orderRepository.deleteAllInBatch();
    }

    @Test
    void shouldPersistOrderTogetherWithOutboxEvent() throws Exception {
        // given
        var request = new CreateOrderRequest(CUSTOMER_ID, PRODUCT_ID, 2, 49.99);

        // when
        String responseBody = mockMvc.perform(post("/orders")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                // then
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").isNotEmpty())
                .andExpect(jsonPath("$.status").value(OrderStatus.PENDING.name()))
                .andExpect(jsonPath("$.totalPrice").value(99.98))
                .andReturn().getResponse().getContentAsString();

        UUID orderId = UUID.fromString(objectMapper.readTree(responseBody).get("orderId").asText());

        assertThat(orderRepository.findById(orderId)).hasValueSatisfying(order -> {
            assertThat(order.getCustomerId()).isEqualTo(CUSTOMER_ID);
            assertThat(order.getProductId()).isEqualTo(PRODUCT_ID);
            assertThat(order.getQuantity()).isEqualTo(2);
            assertThat(order.getTotalPrice()).isEqualByComparingTo("99.98");
            assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
            assertThat(order.getCreatedAt()).isNotNull();
        });

        assertThat(outboxEventRepository.findAll()).singleElement().satisfies(outboxEvent -> {
            assertThat(outboxEvent.getAggregateType()).isEqualTo("Order");
            assertThat(outboxEvent.getAggregateId()).isEqualTo(orderId);
            assertThat(outboxEvent.getEventType()).isEqualTo("OrderCreatedEvent");
            assertThat(outboxEvent.getKafkaTopic()).isEqualTo("order.created");

            OrderCreatedEvent payload = objectMapper.readValue(outboxEvent.getPayload(), OrderCreatedEvent.class);
            assertThat(payload.eventId()).isNotNull();
            assertThat(payload.occurredAt()).isNotNull();
            assertThat(payload)
                    .usingRecursiveComparison()
                    .ignoringFields("eventId", "occurredAt")
                    .isEqualTo(new OrderCreatedEvent(null, orderId, CUSTOMER_ID, PRODUCT_ID, 2, null));
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidRequests")
    void shouldRejectInvalidRequestWithoutPersistingAnything(CreateOrderRequest request) throws Exception {
        // when
        mockMvc.perform(post("/orders")
                        .contentType(APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                // then
                .andExpect(status().isBadRequest());

        assertThat(orderRepository.count()).isZero();
        assertThat(outboxEventRepository.count()).isZero();
    }

    @Test
    void shouldRejectMalformedJson() throws Exception {
        // when
        mockMvc.perform(post("/orders")
                        .contentType(APPLICATION_JSON)
                        .content("{\"customerId\": "))
                // then
                .andExpect(status().isBadRequest());

        assertThat(orderRepository.count()).isZero();
    }

    static Stream<Arguments> invalidRequests() {
        return Stream.of(
                arguments(named("blank customerId", new CreateOrderRequest(" ", PRODUCT_ID, 1, 10.0))),
                arguments(named("missing productId", new CreateOrderRequest(CUSTOMER_ID, null, 1, 10.0))),
                arguments(named("zero quantity", new CreateOrderRequest(CUSTOMER_ID, PRODUCT_ID, 0, 10.0))),
                arguments(named("negative quantity", new CreateOrderRequest(CUSTOMER_ID, PRODUCT_ID, -1, 10.0))),
                arguments(named("zero price", new CreateOrderRequest(CUSTOMER_ID, PRODUCT_ID, 1, 0.0))),
                arguments(named("negative price", new CreateOrderRequest(CUSTOMER_ID, PRODUCT_ID, 1, -5.0)))
        );
    }
}
