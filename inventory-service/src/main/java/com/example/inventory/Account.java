package com.example.inventory;

public class Account {
    private int balance;
    Account(int balance) {
        if(balance < 0) {
            throw new IllegalArgumentException("Balance should be positive");
        }
        this.balance = balance;
    }
    int getBalance() { return balance; }
    void withdraw(int amount) {
        if(amount == 0) throw new IllegalArgumentException("Amount should not be zero");
        if (amount > balance) throw new IllegalArgumentException("Insufficient funds");
        if (amount < 0) throw new IllegalArgumentException("Negative amount");
        balance -= amount;
    }
}
