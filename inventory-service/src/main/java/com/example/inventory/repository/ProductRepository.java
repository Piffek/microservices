package com.example.inventory.repository;

import com.example.inventory.domain.Product;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface ProductRepository extends JpaRepository<Product, String> {

    /**
     * Pobiera produkt z blokadą pesymistyczną (SELECT FOR UPDATE).
     * Zapobiega race condition gdy dwa zamówienia próbują zarezerwować
     * ostatni egzemplarz jednocześnie.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM Product p WHERE p.id = :id")
    Optional<Product> findByIdWithLock(@Param("id") String id);
}
