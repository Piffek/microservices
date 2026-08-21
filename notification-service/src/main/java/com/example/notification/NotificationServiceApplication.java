package com.example.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * NOTIFICATION SERVICE — wysyła powiadomienia do klientów.
 *
 * Stateless (bez własnej bazy) — reaguje na eventy i wywołuje zewnętrzne API mailowe.
 * Demonstracja: Circuit Breaker + Retry + Bulkhead chroniące przed awarią
 * zewnętrznego serwisu email. @EnableScheduling wymagane przez PendingEmailScheduler,
 * który ponawia maile odłożone przez fallback CB.
 */
@SpringBootApplication
@EnableScheduling
public class NotificationServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
