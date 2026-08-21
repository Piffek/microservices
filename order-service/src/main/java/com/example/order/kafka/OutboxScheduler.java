package com.example.order.kafka;

import com.example.order.domain.OutboxEvent;
import com.example.order.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * OUTBOX SCHEDULER — "listonosz" który dostarcza eventy do Kafki.
 *
 * PRZEPŁYW:
 * 1. Co N sekund (fixedDelay) odczytuje niepublikowane eventy z outbox_events
 * 2. Dla każdego eventu: wysyła do Kafki
 * 3. Oznacza event jako opublikowany (published = true)
 *
 * KOLEJNOŚĆ OPERACJI (ważna!):
 * - Najpierw wyślij do Kafki, POTEM oznacz jako published.
 * - Jeśli wysyłka się uda, ale oznaczenie nie (np. crash):
 *   przy następnym uruchomieniu event zostanie wysłany ponownie.
 *   → Duplicate! Dlatego konsument musi być idempotentny (Inbox Pattern).
 * - Jeśli najpierw oznaczysz, potem wyślesz i wyśle się nie uda:
 *   event przepadnie na zawsze. Gorszy scenariusz!
 *
 * ALTERNATYWY dla schedulera:
 * - Debezium (CDC — Change Data Capture): nasłuchuje WAL PostgreSQL i automatycznie
 *   przekazuje zmiany do Kafki. Bardziej niezawodne, ale wymaga dodatkowego komponentu.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxScheduler {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * fixedDelay = 2000ms — czeka 2 sekundy PO zakończeniu poprzedniego wywołania.
     * Nie nakłada się na siebie nawet przy wolnej Kafce.
     * initialDelay = 5000ms — daje czas na start aplikacji.
     */
    @Scheduled(fixedDelay = 2000, initialDelay = 5000)
    @Transactional
    public void processOutboxEvents() {
        List<OutboxEvent> events = outboxEventRepository.findUnpublishedEventsWithLock();

        if (events.isEmpty()) {
            return;
        }

        log.info("OutboxScheduler: found {} unpublished events", events.size());

        for (OutboxEvent event : events) {
            try {
                // Wysyłamy do Kafki synchronicznie (get() blokuje)
                // W produkcji możesz użyć sendDefault().get() z timeoutem
                kafkaTemplate.send(event.getKafkaTopic(), event.getAggregateId().toString(), event.getPayload())
                        .get(); // blokuje do potwierdzenia od brokera

                // Oznacz jako opublikowany DOPIERO po potwierdzeniu od Kafki
                event.setPublished(true);
                event.setPublishedAt(LocalDateTime.now());
                outboxEventRepository.save(event);

                log.info("OutboxEvent published: id={}, topic={}", event.getId(), event.getKafkaTopic());

            } catch (Exception e) {
                // Nie przerywamy pętli — spróbujemy resztę eventów
                // Ten event zostanie ponowiony przy następnym uruchomieniu schedulera
                log.error("Failed to publish outbox event id={}: {}", event.getId(), e.getMessage());
            }
        }
    }
}
