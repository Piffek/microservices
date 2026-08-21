package com.example.inventory.kafka;

import com.example.common.events.OrderCreatedEvent;
import com.example.inventory.service.InventoryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Konsument Kafki — nasłuchuje na topic "order.created".
 *
 * KAFKA CONSUMER GROUPS:
 * groupId = "inventory-service-group"
 * Każda partycja jest przypisana do dokładnie jednego konsumenta w grupie.
 * Jeśli uruchomisz 3 instancje Inventory Service (skalowanie),
 * Kafka rozdzieli 3 partycje między 3 instancje — przetwarzanie równoległe!
 * Gdybyś uruchomił 4 instancje, jedna z nich byłaby bezczynna (więcej konsumentów niż partycji).
 *
 * MANUAL ACKNOWLEDGMENT (ack-mode: manual):
 * Domyślnie Kafka commituje offset automatycznie po odebraniu wiadomości.
 * To niebezpieczne! Jeśli przetwarzanie się wywróci PO commicie offsetu,
 * wiadomość zostanie utracona (lost message).
 *
 * Przy manual ack: acknowledgment.acknowledge() jest wywoływany TYLKO po
 * pomyślnym przetworzeniu. Jeśli przetwarzanie rzuci wyjątek, offset
 * NIE jest commitowany i Kafka prześle wiadomość ponownie (retry).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final InventoryService inventoryService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "order.created",
            groupId = "inventory-service-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        log.info("Received message from Kafka: topic={}, partition={}, offset={}, key={}",
                record.topic(), record.partition(), record.offset(), record.key());

        try {
            OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);
            inventoryService.processOrderCreated(event);

            // Commit offset TYLKO po pomyślnym przetworzeniu
            acknowledgment.acknowledge();
            log.info("Message acknowledged: eventId={}", event.eventId());

        } catch (Exception e) {
            // NIE commitujemy offsetu — Kafka prześle wiadomość ponownie
            // W produkcji: po N próbach wiadomość trafia do Dead Letter Topic (DLT)
            log.error("Failed to process OrderCreatedEvent: {}", e.getMessage(), e);
            // Rzucamy dalej żeby Spring Kafka obsłużył retry (lub DLT)
            throw new RuntimeException("Failed to process event", e);
        }
    }
}
