package com.example.inventory;

class WithdrawalService {
    private final AccountRepository repository;

    WithdrawalService(AccountRepository repository) {
        this.repository = repository;
    }

    void withdraw(String accountId, int amount) {
        Account account = repository.findById(accountId)
                .orElseThrow(() -> new IllegalArgumentException("Account not found"));
        account.withdraw(amount);
        repository.save(account);
    }
}
