package com.example.notification.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Kolejka maili odłożonych do ponowienia — fallback Circuit Breakera trafia tutaj
 * zamiast tylko logować i zapominać o wiadomości.
 *
 * UWAGA: to jest kolejka W PAMIĘCI, nie w bazie danych. notification-service jest
 * celowo bezstanowy (patrz NotificationServiceApplication), więc restart appki
 * czyści tę kolejkę. W produkcji byłaby to tabela, tak jak outbox_events w
 * order-service — dokładnie ten sam pomysł, inna skala gwarancji.
 */
@Slf4j
@Component
public class PendingEmailQueue {

    public record PendingEmail(String to, String subject, String body) {}

    private final Queue<PendingEmail> queue = new ConcurrentLinkedQueue<>();

    public void enqueue(String to, String subject, String body) {
        queue.add(new PendingEmail(to, subject, body));
        log.info("Email do {} odłożony do ponowienia. W kolejce: {}", to, queue.size());
    }

    public PendingEmail poll() {
        return queue.poll();
    }

    public int size() {
        return queue.size();
    }
}
