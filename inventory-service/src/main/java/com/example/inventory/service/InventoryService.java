package com.example.inventory.service;

import com.example.common.events.InventoryReservedEvent;
import com.example.common.events.OrderCreatedEvent;
import com.example.inventory.domain.InboxEvent;
import com.example.inventory.domain.Product;
import com.example.inventory.kafka.InventoryEventPublisher;
import com.example.inventory.repository.InboxEventRepository;
import com.example.inventory.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Logika biznesowa Inventory Service.
 *
 * Metoda processOrderCreated() implementuje Inbox Pattern:
 *   1. Sprawdź czy event był już przetworzony (idempotentność)
 *   2. Przetwórz event + zapisz do inbox (atomowo w jednej transakcji)
 *   3. Wyemituj event z wynikiem
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private final ProductRepository productRepository;
    private final InboxEventRepository inboxEventRepository;
    private final InventoryEventPublisher eventPublisher;

    @Transactional
    public void processOrderCreated(OrderCreatedEvent event) {
        // === INBOX PATTERN: Krok 1 — sprawdź idempotentność ===
        if (inboxEventRepository.existsByEventId(event.eventId())) {
            log.warn("Duplicate event detected, skipping. eventId={}", event.eventId());
            return; // event już przetworzony — bezpiecznie ignorujemy
        }

        log.info("Processing OrderCreatedEvent: eventId={}, orderId={}, productId={}, qty={}",
                event.eventId(), event.orderId(), event.productId(), event.quantity());

        // === Krok 2 — przetwórz biznesowo ===
        InventoryReservedEvent resultEvent;

        Product product = productRepository.findByIdWithLock(event.productId())
                .orElse(null);

        if (product == null) {
            log.warn("Product not found: productId={}", event.productId());
            resultEvent = InventoryReservedEvent.failure(
                    event.orderId(), event.customerId(), "Product not found: " + event.productId()
            );
        } else if (product.reserve(event.quantity())) {
            productRepository.save(product);
            log.info("Stock reserved: productId={}, qty={}, remainingStock={}",
                    product.getId(), event.quantity(), product.getStock());
            resultEvent = InventoryReservedEvent.success(event.orderId(), event.customerId());
        } else {
            log.warn("Insufficient stock: productId={}, requested={}, available={}",
                    product.getId(), event.quantity(), product.getStock());
            resultEvent = InventoryReservedEvent.failure(
                    event.orderId(), event.customerId(),
                    "Insufficient stock. Available: " + product.getStock() + ", requested: " + event.quantity()
            );
        }

        // === INBOX PATTERN: Krok 3 — zapisz do inbox W TEJ SAMEJ TRANSAKCJI ===
        // Jeśli transakcja się wywróci, NIE zapiszemy do inbox,
        // więc przy następnej dostawie tego eventu, przetworzymy go ponownie.
        // Dopóki transakcja nie zostanie scommitowana, event nie jest "widziany" jako przetworzony.
        inboxEventRepository.save(InboxEvent.of(event.eventId(), "OrderCreatedEvent"));

        // === Krok 4 — wyemituj event z wynikiem PO commicie transakcji DB ===
        // Dzięki temu Notification Service nie dostanie eventu jeśli commit się nie powiedzie.
        
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                eventPublisher.publishInventoryReserved(resultEvent);
            }
        });
    }
}
