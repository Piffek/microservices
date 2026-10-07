package com.example.inventory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class AccountTest {

    @ParameterizedTest
    @ValueSource(ints = {-1, -100})
    void shouldBalanceBePositive(int balance) {
        //when
        var ex = assertThrows(IllegalArgumentException.class, () -> new Account(balance));
        //then
        assertEquals("Balance should be positive", ex.getMessage());
    }

    @Test
    void shouldBalanceBeZero() {
        //given
        var account = new Account(0);
        //when
        var currentBalance = account.getBalance();
        //then
        assertEquals(0, currentBalance);

    }

    @Nested
    class Withdraw {
        Account account;

        @BeforeEach
        void setUp() {
            account = new Account(100);
        }

        @ParameterizedTest
        @CsvSource({
                "30, 70",
                "100, 0"
        })
        void shouldCalculateAccountWithdrawAmount(int amount, int expectedBalance) {
            //when
            account.withdraw(amount);
            //then
            assertEquals(expectedBalance, account.getBalance());
        }

        @Test
        void shouldRejectWithdrawForAmountZero() {
            //when
            var ex = assertThrows(IllegalArgumentException.class, () -> account.withdraw(0));
            //then
            assertAll(
                    () -> assertEquals("Amount should not be zero", ex.getMessage()),
                    () -> assertEquals(100, account.getBalance())
            );
        }

        @ParameterizedTest(name = "amount {0} should throw exception")
        @ValueSource(ints = {-1, -100, -50})
        void shouldRejectWithdrawForNegativeAmount(int amount) {
            //when
            var ex = assertThrows(IllegalArgumentException.class, () -> account.withdraw(amount));
            //then
            assertAll(
                    () -> assertEquals("Negative amount", ex.getMessage()),
                    () -> assertEquals(100, account.getBalance())
            );
        }

        @ParameterizedTest(name = "{0} should throw exception because of Insufficient funds")
        @ValueSource(ints = {200, 101})
        void shouldRejectWithdrawExceedingBalance(int amount) {
            //when
            var ex = assertThrows(IllegalArgumentException.class, () -> account.withdraw(amount));
            //then
            assertAll(
                    () -> assertEquals("Insufficient funds", ex.getMessage()),
                    () -> assertEquals(100, account.getBalance())
            );
        }
    }

}
