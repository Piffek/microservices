package com.example.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * INVENTORY SERVICE — zarządza stanami magazynowymi.
 *
 * Reaguje na eventy "order.created" i rezerwuje towar.
 * Emituje eventy "inventory.reserved" z wynikiem operacji.
 */
@SpringBootApplication
public class InventoryServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
