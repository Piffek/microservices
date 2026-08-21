package com.example.order.controller.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

/**
 * DTO dla żądania tworzenia zamówienia.
 * Record = immutable, automatyczny equals/hashCode/toString.
 */
public record CreateOrderRequest(

        @NotBlank(message = "customerId cannot be blank")
        String customerId,

        @NotBlank(message = "productId cannot be blank")
        String productId,

        @Min(value = 1, message = "quantity must be at least 1")
        int quantity,

        @Positive(message = "pricePerUnit must be positive")
        double pricePerUnit

) {}
