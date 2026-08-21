package com.example.notification.service;

import com.example.common.events.InventoryReservedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Orkiestruje wysyłanie powiadomień na podstawie eventów z Kafki.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final EmailClient emailClient;

    public void handleInventoryReserved(InventoryReservedEvent event) {
        String recipient = event.customerId() + "@example.com"; // symulacja

        if (event.success()) {
            log.info("Sending ORDER CONFIRMED notification: orderId={}, customer={}",
                    event.orderId(), event.customerId());
            emailClient.sendEmail(
                    recipient,
                    "Order Confirmed #" + event.orderId(),
                    "Great news! Your order has been confirmed and stock has been reserved. " +
                            "Order ID: " + event.orderId()
            );
        } else {
            log.info("Sending ORDER CANCELLED notification: orderId={}, customer={}, reason={}",
                    event.orderId(), event.customerId(), event.message());
            emailClient.sendEmail(
                    recipient,
                    "Order Issue #" + event.orderId(),
                    "Unfortunately we cannot process your order. Reason: " + event.message()
            );
        }
    }
}
