package com.example.order.controller;

import com.example.order.controller.dto.CreateOrderRequest;
import com.example.order.domain.Order;
import com.example.order.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST kontroler Order Service.
 *
 * Endpoint: POST /orders
 * Przyjmuje żądanie złożenia zamówienia i zwraca ID zamówienia.
 *
 * Przykład użycia (curl):
 * curl -X POST http://localhost:8081/orders \
 *   -H "Content-Type: application/json" \
 *   -d '{"customerId":"user-1","productId":"prod-123","quantity":2,"pricePerUnit":49.99}'
 */
@Slf4j
@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<Map<String, Object>> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        log.info("Received order request: customerId={}, productId={}, qty={}",
                request.customerId(), request.productId(), request.quantity());

        Order order = orderService.createOrder(request);

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "orderId", order.getId(),
                "status", order.getStatus(),
                "totalPrice", order.getTotalPrice(),
                "message", "Order accepted. Processing asynchronously."
        ));
    }
}
