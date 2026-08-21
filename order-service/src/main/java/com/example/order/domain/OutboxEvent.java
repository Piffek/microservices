package com.example.order.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * OUTBOX PATTERN — serce niezawodnej komunikacji z Kafką.
 *
 * PROBLEM KTÓRY ROZWIĄZUJE:
 * Wyobraź sobie, że zapisujesz Order do bazy i natychmiast
 * wysyłasz event do Kafki. Co jeśli:
 *   a) Zapis do DB się uda, ale Kafka jest niedostępna?
 *      → Event przepada, Inventory nie dowie się o zamówieniu!
 *   b) Kafka dostanie event, ale DB commit się wywróci?
 *      → Phantom event — Inventory przetworzy zamówienie, którego nie ma!
 *
 * ROZWIĄZANIE — Outbox Pattern:
 * 1. W JEDNEJ TRANSAKCJI zapisz: Order + OutboxEvent do bazy.
 *    Albo oba zapisane, albo żadne — atomowość gwarantuje ACID bazy danych.
 * 2. Scheduler (OutboxScheduler) odczytuje niepublikowane eventy
 *    i wysyła je do Kafki ASYNCHRONICZNIE.
 * 3. Po udanym wysłaniu, oznaczasz event jako opublikowany (published = true).
 *
 * GWARANCJA: AT-LEAST-ONCE delivery (event dotrze co najmniej raz).
 * Może dotrzeć więcej niż raz (restart przed aktualizacją published=true)
 * — dlatego konsument musi być idempotentny (Inbox Pattern!).
 */
@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    /**
     * Typ agregatu, którego dotyczy event.
     * Przydatne gdy kilka agregatów dzieli jedną tabelę outbox (rzadkie, ale możliwe).
     */
    @Column(nullable = false)
    private String aggregateType;

    /** ID agregatu (np. order.getId()) */
    @Column(nullable = false)
    private UUID aggregateId;

    /** Nazwa klasy eventu — pozwala konsumentowi dobrać właściwy handler */
    @Column(nullable = false)
    private String eventType;

    /** Payload eventu jako JSON — elastyczny format, łatwy do debugowania */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    /** Topic Kafki, na który należy wysłać event */
    @Column(nullable = false)
    private String kafkaTopic;

    /** Flaga: czy event został już wysłany do Kafki? */
    @Column(nullable = false)
    private boolean published;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    private LocalDateTime publishedAt;

    @PrePersist
    void onCreate() {
        this.createdAt = LocalDateTime.now();
        this.published = false;
    }
}
