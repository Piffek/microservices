package com.example.order.repository;

import com.example.order.domain.OutboxEvent;
import com.example.order.support.TestContainers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test zapytania natywnego z FOR UPDATE SKIP LOCKED — składni specyficznej dla PostgreSQL,
 * której nie da się wiarygodnie przetestować na H2. Dlatego prawdziwa baza w kontenerze.
 *
 * Slice test (@DataJpaTest) dostaje WŁASNĄ instancję PostgreSQL, a nie współdzieloną:
 * w pełnym kontekście aplikacji działa OutboxScheduler, który publikowałby i blokował
 * eventy tworzone tutaj, a create-drop schematu tego kontekstu kasowałby tabele
 * pod działającym schedulerem.
 *
 * NOT_SUPPORTED wyłącza domyślną transakcję testu — tutaj sterujemy transakcjami jawnie,
 * bo testujemy właśnie zachowanie dwóch równoległych transakcji.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Testcontainers
class OutboxEventRepositoryIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(TestContainers.POSTGRES_IMAGE);

    @Autowired
    private OutboxEventRepository repository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void setUp() {
        repository.deleteAllInBatch();
        transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Test
    void shouldReturnOnlyUnpublishedEvents() {
        // given
        OutboxEvent unpublished = repository.save(newOutboxEvent());
        OutboxEvent published = repository.save(newOutboxEvent());
        published.setPublished(true);
        repository.save(published);

        // when
        List<OutboxEvent> result = inTransaction(repository::findUnpublishedEventsWithLock);

        // then
        assertThat(result)
                .extracting(OutboxEvent::getId)
                .containsExactly(unpublished.getId());
    }

    @Test
    void shouldSkipEventsLockedByAnotherTransaction() {
        // given
        repository.saveAll(List.of(newOutboxEvent(), newOutboxEvent()));
        List<OutboxEvent> lockedByFirstInstance = new ArrayList<>();

        // when — pierwsza "instancja" trzyma blokadę, druga odpytuje w tym samym czasie
        List<OutboxEvent> visibleToSecondInstance = inTransaction(() -> {
            lockedByFirstInstance.addAll(repository.findUnpublishedEventsWithLock());
            return findUnpublishedInConcurrentTransaction();
        });

        // then
        assertThat(lockedByFirstInstance).hasSize(2);
        assertThat(visibleToSecondInstance).isEmpty();
    }

    @Test
    void shouldReturnEventsAgainOnceLockIsReleased() {
        // given
        repository.saveAll(List.of(newOutboxEvent(), newOutboxEvent()));
        inTransaction(repository::findUnpublishedEventsWithLock);

        // when
        List<OutboxEvent> result = findUnpublishedInConcurrentTransaction();

        // then
        assertThat(result).hasSize(2);
    }

    /**
     * Gdyby zapytanie straciło SKIP LOCKED, druga transakcja zawisłaby na blokadzie
     * zamiast zwrócić pustą listę — timeout zamienia to w czytelny błąd zamiast wiszącego builda.
     */
    private List<OutboxEvent> findUnpublishedInConcurrentTransaction() {
        return CompletableFuture
                .supplyAsync(() -> inTransaction(repository::findUnpublishedEventsWithLock))
                .orTimeout(5, TimeUnit.SECONDS)
                .join();
    }

    private <T> T inTransaction(Supplier<T> action) {
        return transactionTemplate.execute(status -> action.get());
    }

    private static OutboxEvent newOutboxEvent() {
        return OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId(UUID.randomUUID())
                .eventType("OrderCreatedEvent")
                .payload("{}")
                .kafkaTopic("order.created")
                .build();
    }
}
