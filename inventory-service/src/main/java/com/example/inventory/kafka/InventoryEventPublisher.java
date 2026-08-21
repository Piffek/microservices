package com.example.inventory.kafka;

import com.example.common.events.InventoryReservedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publikuje eventy Inventory Service do Kafki.
 *
 * Uwaga: w tym serwisie NIE używamy Outbox Pattern dla uproszczenia.
 * W produkcji powinieneś rozważyć Outbox też tutaj (jeśli potrzebujesz
 * gwarancji at-least-once delivery dla inventory.reserved eventów).
 *
 * Dla Notification Service duplikaty są mniej krytyczne (podwójny email
 * jest mniej poważny niż brak emaila), więc można zaakceptować fire-and-forget.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void publishInventoryReserved(InventoryReservedEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            // Klucz = orderId → ta sama partycja dla eventów jednego zamówienia
            kafkaTemplate.send("inventory.reserved", event.orderId().toString(), payload);
            log.info("Published InventoryReservedEvent: orderId={}, success={}", event.orderId(), event.success());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize InventoryReservedEvent: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
