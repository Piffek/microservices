package com.example.notification.kafka;

import com.example.common.events.InventoryReservedEvent;
import com.example.notification.service.NotificationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Konsument eventów z Inventory Service.
 * Nasłuchuje na topic "inventory.reserved".
 *
 * Dla uproszczenia: auto-commit offsetu (w produkcji użyj manual ack jak w Inventory Service).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryEventConsumer {

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "inventory.reserved",
            groupId = "notification-service-group"
    )
    public void onInventoryReserved(ConsumerRecord<String, String> record) {
        log.info("Received InventoryReservedEvent: partition={}, offset={}", record.partition(), record.offset());
        try {
            InventoryReservedEvent event = objectMapper.readValue(record.value(), InventoryReservedEvent.class);
            notificationService.handleInventoryReserved(event);
        } catch (Exception e) {
            log.error("Failed to process InventoryReservedEvent: {}", e.getMessage(), e);
        }
    }
}
