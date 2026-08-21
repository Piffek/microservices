package com.example.order.service;

import com.example.common.events.OrderCreatedEvent;
import com.example.order.controller.dto.CreateOrderRequest;
import com.example.order.domain.Order;
import com.example.order.domain.OutboxEvent;
import com.example.order.repository.OrderRepository;
import com.example.order.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

/**
 * Główna logika biznesowa Order Service.
 *
 * KLUCZOWY MOMENT: metoda createOrder jest @Transactional.
 * W JEDNEJ TRANSAKCJI:
 *   1. Zapisuje Order do tabeli "orders"
 *   2. Zapisuje OutboxEvent do tabeli "outbox_events"
 *
 * Jeśli cokolwiek się nie uda (np. błąd bazy), OBA zapisy są cofane.
 * Jeśli wszystko gra, OBA są commitowane atomowo.
 *
 * To jest sedno Outbox Pattern — DB jako pośrednik niezawodności.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private static final String ORDER_CREATED_TOPIC = "order.created";

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public Order createOrder(CreateOrderRequest request) {
        // Krok 1: Utwórz i zapisz zamówienie
        Order order = Order.builder()
                .customerId(request.customerId())
                .productId(request.productId())
                .quantity(request.quantity())
                .totalPrice(BigDecimal.valueOf(request.pricePerUnit()).multiply(BigDecimal.valueOf(request.quantity())))
                .build();
        orderRepository.save(order);

        log.info("Order created: orderId={}, customerId={}, productId={}",
                order.getId(), order.getCustomerId(), order.getProductId());

        // Krok 2: W TEJ SAMEJ TRANSAKCJI utwórz OutboxEvent
        OrderCreatedEvent event = OrderCreatedEvent.of(
                order.getId(),
                order.getCustomerId(),
                order.getProductId(),
                order.getQuantity()
        );

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId(order.getId())
                .eventType("OrderCreatedEvent")
                .payload(toJson(event))
                .kafkaTopic(ORDER_CREATED_TOPIC)
                .build();
        outboxEventRepository.save(outboxEvent);

        log.info("OutboxEvent created: eventId={} for orderId={}", outboxEvent.getId(), order.getId());

        // Transakcja zostanie scommitowana po wyjściu z metody.
        // OutboxScheduler wykryje nowy event i wyśle go do Kafki.
        return order;
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event to JSON", e);
        }
    }
}
