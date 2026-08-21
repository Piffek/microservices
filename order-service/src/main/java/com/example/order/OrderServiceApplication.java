package com.example.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * ORDER SERVICE — punkt wejścia.
 *
 * Odpowiedzialność: przyjmowanie zamówień od klientów.
 *
 * @EnableScheduling — włącza mechanizm schedulera Springa,
 * potrzebny dla OutboxScheduler (@Scheduled).
 */
@SpringBootApplication
@EnableScheduling
public class OrderServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
