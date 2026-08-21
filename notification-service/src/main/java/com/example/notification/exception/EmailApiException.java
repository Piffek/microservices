package com.example.notification.exception;

/**
 * Błąd INFRASTRUKTURY zewnętrznego API mailowego (timeout, connection refused, 5xx).
 * To jest jedyny typ błędu, który powinien liczyć się do statystyk Circuit Breakera —
 * patrz resilience4j.circuitbreaker.instances.emailService.record-exceptions w application.yml.
 */
public class EmailApiException extends RuntimeException {
    public EmailApiException(String message) {
        super(message);
    }
}
