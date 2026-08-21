package com.example.inventory.config;

import com.example.inventory.domain.Product;
import com.example.inventory.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Inicjalizuje przykładowe produkty w bazie przy starcie.
 * W produkcji dane załadowałbyś przez Flyway/Liquibase seed scripts.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {

    private final ProductRepository productRepository;

    @Override
    public void run(String... args) {
        if (productRepository.count() == 0) {
            productRepository.save(Product.builder().id("prod-123").name("Laptop Pro 15").stock(10).build());
            productRepository.save(Product.builder().id("prod-456").name("Mechanical Keyboard").stock(50).build());
            productRepository.save(Product.builder().id("prod-789").name("USB-C Hub").stock(0).build()); // brak towaru!
            log.info("Initialized 3 products in inventory");
        }
    }
}
