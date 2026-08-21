package com.example.common.events;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Event emitowany przez Inventory Service po przetworzeniu zamówienia.
 * Może oznaczać sukces (towar zarezerwowany) lub porażkę (brak towaru).
 *
 * DOBRE PRAKTYKI:
 * 1. Event zawiera WYNIK operacji, nie komendę — Notification Service sam decyduje, co zrobić.
 * 2. Pole "success" pozwala konsumentowi obsłużyć oba przypadki.
 * 3. Zawsze dołączamy orderId, żeby konsument wiedział, o jakim zamówieniu mowa.
 */
public record InventoryReservedEvent(

        UUID eventId,
        UUID orderId,
        String customerId,
        boolean success,

        /**
         * Opis wyniku — np. "Stock reserved successfully" lub "Insufficient stock".
         * W produkcji używaj kodów błędów zamiast darmowego tekstu.
         */
        String message,

        @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
        LocalDateTime occurredAt

) {
    public static InventoryReservedEvent success(UUID orderId, String customerId) {
        return new InventoryReservedEvent(
                UUID.randomUUID(), orderId, customerId, true,
                "Stock reserved successfully", LocalDateTime.now()
        );
    }

    public static InventoryReservedEvent failure(UUID orderId, String customerId, String reason) {
        return new InventoryReservedEvent(
                UUID.randomUUID(), orderId, customerId, false,
                reason, LocalDateTime.now()
        );
    }
}
