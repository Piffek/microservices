package com.example.inventory.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * INBOX PATTERN — ochrona przed duplikatami.
 *
 * PROBLEM KTÓRY ROZWIĄZUJE:
 * Kafka gwarantuje AT-LEAST-ONCE delivery — wiadomość może dotrzeć
 * więcej niż raz (np. przy restarcie konsumenta przed commitem offsetu).
 * Bez ochrony ten sam event zostałby przetworzony wielokrotnie:
 *   - zduplikowane rezerwacje towaru
 *   - wysłanie wielu maili do klienta
 *   - etc.
 *
 * ROZWIĄZANIE — Inbox Pattern:
 * Przed przetworzeniem eventu, sprawdź czy event.eventId jest już w inbox.
 *   - Jest? → Event już przetworzony, pomiń (idempotentnie).
 *   - Nie ma? → Przetwórz event i zapisz eventId do inbox (w JEDNEJ TRANSAKCJI).
 *
 * Dzięki temu osiągamy EXACTLY-ONCE semantics na poziomie logiki biznesowej
 * (nawet jeśli Kafka dostarcza AT-LEAST-ONCE na poziomie transportu).
 *
 * Kluczowe: eventId pochodzi z EVENTU (OutboxEvent.id == eventId w evencie),
 * nie jest generowane przez konsumenta. Musi być tym samym UUID co pole
 * eventId w OrderCreatedEvent/InventoryReservedEvent.
 */
@Entity
@Table(name = "inbox_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InboxEvent {

    /**
     * eventId to PRIMARY KEY — baza danych automatycznie odrzuci
     * duplikat przez constraint UNIQUE. To nasz "fuse" idempotentności.
     */
    @Id
    private UUID eventId;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false)
    private LocalDateTime processedAt;

    public static InboxEvent of(UUID eventId, String eventType) {
        return new InboxEvent(eventId, eventType, LocalDateTime.now());
    }
}
