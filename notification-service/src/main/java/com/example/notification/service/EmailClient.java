package com.example.notification.service;

import com.example.notification.exception.EmailApiException;
import com.example.notification.exception.InvalidEmailAddressException;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * CIRCUIT BREAKER — automatyczne wyłączenie "zepsutego" serwisu.
 *
 * Symulujemy zawodne zewnętrzne API emailowe (np. SendGrid, Mailgun): część
 * wywołań kończy się szybkim błędem, część "wisi" (symulacja timeoutu sieciowego)
 * — celowo, żeby zademonstrować zarówno failure-rate-threshold jak i
 * slow-call-duration-threshold w application.yml.
 *
 * STANY CIRCUIT BREAKERA:
 *
 *   [CLOSED] ──błędy/wolne wywołania > threshold──► [OPEN] ──waitDuration──► [HALF_OPEN]
 *      ▲                                                                          │
 *      └───────────────────────── sukces ───────────────────────────────────────┘
 *
 * 1. CLOSED (zamknięty = normalny przepływ):
 *    Wszystkie wywołania przechodzą do EmailAPI. CB zlicza błędy ORAZ wywołania
 *    wolniejsze niż slow-call-duration-threshold — jedno i drugie liczy się do progu.
 *
 * 2. OPEN (otwarty = wyłącznik zadziałał):
 *    Wywołania są NATYCHMIAST odrzucane (CallNotPermittedException, bez wywoływania
 *    EmailAPI) → leci do fallbacku. Chroni appkę przed marnowaniem wątków na
 *    wywołania, które i tak się nie udadzą.
 *
 * 3. HALF_OPEN (w połowie otwarty = testowanie):
 *    CB przepuszcza permitted-number-of-calls-in-half-open-state próbnych wywołań.
 *    Sukces → CLOSED. Błąd → z powrotem OPEN.
 *
 * WARSTWY OCHRONY (od zewnątrz do środka):
 *   @Retry     — przejściowy błąd dostaje jeszcze 2 dodatkowe szanse (z backoffem),
 *                ale NIE próbuje ponownie gdy CB jest OPEN (patrz ignore-exceptions
 *                dla CallNotPermittedException w application.yml — inaczej retry
 *                dobijałby otwarty CB zamiast dać mu odpocząć).
 *   @CircuitBreaker — bezpiecznik opisany wyżej.
 *   @Bulkhead  — maks. 10 równoległych wywołań emailService, żeby wolne wysyłki
 *                nie zjadły całej puli wątków appki.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EmailClient {

    private final Random random = new Random();
    private final PendingEmailQueue pendingEmailQueue;

    @Retry(name = "emailService")
    @CircuitBreaker(name = "emailService", fallbackMethod = "sendEmailFallback")
    @Bulkhead(name = "emailService")
    public void sendEmail(String to, String subject, String body) {
        validateEmailAddress(to);
        simulateUnreliableEmailApi(to, subject);
        log.info("Email sent successfully to: {}", to);
    }

    /**
     * FALLBACK METHOD — wywoływana gdy CB jest OPEN, retry się wyczerpie,
     * albo API rzuci wyjątek.
     *
     * Błąd walidacji (zły adres) NIE trafia do kolejki ponowień — zły adres
     * nie stanie się poprawny przy kolejnej próbie, więc tylko logujemy i
     * porzucamy. Każdy inny błąd (infrastruktura, CB OPEN) ląduje w
     * PendingEmailQueue, skąd PendingEmailScheduler spróbuje ponownie później.
     */
    public void sendEmailFallback(String to, String subject, String body, Throwable ex) {
        if (ex instanceof InvalidEmailAddressException) {
            log.error("Email do {} odrzucony — nieprawidłowy adres, nie ponawiamy: {}", to, ex.getMessage());
            return;
        }

        if (ex instanceof CallNotPermittedException) {
            log.warn("CIRCUIT BREAKER OPEN — email do {} pominięty bez próby wysyłki. Odkładam do ponowienia.", to);
        } else {
            log.warn("Wysyłka maila do {} nie powiodła się: {}. Odkładam do ponowienia.", to, ex.getMessage());
        }

        pendingEmailQueue.enqueue(to, subject, body);
    }

    private void validateEmailAddress(String to) {
        if (to == null || !to.contains("@")) {
            throw new InvalidEmailAddressException("Nieprawidłowy adres email: " + to);
        }
    }

    private void simulateUnreliableEmailApi(String to, String subject) {
        int outcome = random.nextInt(10);

        if (outcome < 3) {
            // 30% — szybki błąd (np. connection refused)
            sleep(100);
            log.debug("Email API simulated fast failure for: {}", to);
            throw new EmailApiException("Email API connection refused");
        } else if (outcome < 5) {
            // 20% — wywołanie "wisi" dłużej niż slow-call-duration-threshold (2s) i się wywala
            sleep(2500);
            log.debug("Email API simulated slow failure for: {}", to);
            throw new EmailApiException("Email API timeout after slow response");
        } else if (outcome < 6) {
            // 10% — wolne, ale ostatecznie się udaje; mimo sukcesu CB liczy to jako "slow call"
            sleep(2500);
        } else {
            // 40% — szybki sukces
            sleep(100);
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
