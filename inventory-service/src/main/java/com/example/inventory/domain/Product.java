package com.example.inventory.domain;

import jakarta.persistence.*;
import lombok.*;

/**
 * Produkt w magazynie.
 * Śledzi ilość dostępnego towaru (stock).
 *
 * W produkcji do aktualizacji stanu używałbyś operacji atomowych
 * (np. UPDATE products SET stock = stock - ? WHERE id = ? AND stock >= ?)
 * żeby uniknąć race conditions przy równoległych zamówieniach.
 */
@Entity
@Table(name = "products")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Product {

    @Id
    private String id;  // np. "prod-123" — używamy tego samego ID co w Order

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private int stock;

    /**
     * Rezerwuje podaną ilość towaru.
     * @return true jeśli udało się zarezerwować, false jeśli brak towaru.
     */
    public boolean reserve(int quantity) {
        if (this.stock < quantity) {
            return false;
        }
        this.stock -= quantity;
        return true;
    }
}
