package com.example.inventory.repository;

import com.example.inventory.domain.InboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface InboxEventRepository extends JpaRepository<InboxEvent, UUID> {

    /** Sprawdza czy event o danym ID był już przetworzony. */
    boolean existsByEventId(UUID eventId);
}
