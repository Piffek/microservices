package com.example.notification.exception;

/**
 * Błąd WALIDACJI danych wejściowych — zły format adresu email.
 * To NIE jest awaria zewnętrznego API, więc świadomie wykluczamy ten wyjątek
 * z liczenia do progu Circuit Breakera (ignore-exceptions w application.yml).
 * Ponawianie takiego wywołania (Retry) też nie ma sensu — zły adres nie stanie
 * się poprawny przy kolejnej próbie.
 */
public class InvalidEmailAddressException extends RuntimeException {
    public InvalidEmailAddressException(String message) {
        super(message);
    }
}
