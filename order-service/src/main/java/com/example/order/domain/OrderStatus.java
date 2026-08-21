package com.example.order.domain;

/**
 * Status zamówienia.
 *
 * Przejścia stanów:
 *   PENDING → CONFIRMED  (gdy Inventory zarezerwuje towar)
 *   PENDING → CANCELLED  (gdy Inventory nie ma towaru)
 *
 * W tej wersji Order Service NIE aktualizuje statusu bezpośrednio —
 * robiłby to dopiero gdyby nasłuchiwał na inventory.reserved event
 * (co możesz dodać jako ćwiczenie!).
 */
public enum OrderStatus {
    PENDING,
    CONFIRMED,
    CANCELLED
}
