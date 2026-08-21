package com.example.common.events;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Event emitowany przez Order Service, gdy zamówienie zostanie złożone.
 *
 * ZASADA EVENT-DRIVEN ARCHITECTURE:
 * Serwisy nie wywołują się nawzajem przez HTTP — zamiast tego jeden serwis
 * emituje event, a pozostałe reagują na niego. Dzięki temu:
 *  - Order Service NIE ZALEŻY od Inventory Service (luzna zaleznoss)
 *  - Jeśli Inventory Service padnie, zamówienie i tak zostanie zapisane
 *  - Nowy serwis może "wsłuchać się" w event bez modyfikacji Order Service
 *
 * Używamy Java Record — immutable, bez boilerplate, idealny dla DTO/eventów.
 */
public record OrderCreatedEvent(

        /**
         * Unikalny identyfikator EVENTU (nie zamówienia!).
         * Używany przez Inbox Pattern do sprawdzenia idempotentności.
         * Każdy event powinien mieć swój własny ID, nawet jeśli dotyczy tego samego zamówienia.
         */
        UUID eventId,

        /** ID zamówienia — aggregateId w terminologii Domain-Driven Design */
        UUID orderId,

        String customerId,
        String productId,
        int quantity,

        @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
        LocalDateTime occurredAt

) {
    /**
     * Factory method — dobra praktyka, enkapsuluje tworzenie eventu
     * i zawsze ustawia aktualny czas.
     */
    public static OrderCreatedEvent of(UUID orderId, String customerId, String productId, int quantity) {
        return new OrderCreatedEvent(
                UUID.randomUUID(),
                orderId,
                customerId,
                productId,
                quantity,
                LocalDateTime.now()
        );
    }
}
