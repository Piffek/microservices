package com.example.order.repository;

import com.example.order.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Pobiera niepublikowane eventy z blokadą SKIP LOCKED.
     *
     * DLACZEGO SKIP LOCKED?
     * W środowisku produkcyjnym może działać wiele instancji Order Service.
     * Bez blokady dwie instancje mogłyby pobrać ten sam event i wysłać go
     * do Kafki dwa razy (choć Inbox Pattern by to obsłużył, lepiej unikać).
     *
     * SKIP LOCKED = "pomiń wiersze zablokowane przez inne transakcje"
     * Zamiast czekać (deadlock risk), po prostu pomijamy — druga instancja
     * przetworzy je przy następnym uruchomieniu schedulera.
     *
     * Jest to zapytanie natywne (PostgreSQL-specific syntax).
     */
    @Query(value = "SELECT * FROM outbox_events WHERE published = false ORDER BY created_at FOR UPDATE SKIP LOCKED",
            nativeQuery = true)
    List<OutboxEvent> findUnpublishedEventsWithLock();
}
