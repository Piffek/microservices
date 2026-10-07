package com.example.inventory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class WithdrawalServiceTest {

    @Mock
    AccountRepository accountRepository;

    @InjectMocks
    WithdrawalService withdrawalService;

    @Captor
    ArgumentCaptor<Account> captor;

    @Test
    void shouldChangeAmound() {
        //given
        var accountId = "1";
        var amount = 10;
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(new Account(100)));

        //when
        withdrawalService.withdraw(accountId, amount);

        //then
        verify(accountRepository).save(captor.capture());
        assertEquals(90, captor.getValue().getBalance());
    }

    @Test
    void shouldNotChangeAmoundBecauseAccountNotExists() {
        //given
        var accountId = "1";
        var amount = 10;
        var account = new Account(100);
        when(accountRepository.findById(accountId)).thenReturn(Optional.empty());

        //when
        var ex = assertThrows(IllegalArgumentException.class, () -> withdrawalService.withdraw(accountId, amount));

        //then
        assertAll(
                () -> assertEquals(ex.getMessage(), "Account not found"),
                () -> assertEquals(100, account.getBalance()),
                () -> verify(accountRepository, never()).save(account)
        );
    }

    @Test
    void shouldNotChangeAmound() {
        //given
        var accountId = "1";
        var amount = 200;
        var account = new Account(100);
        when(accountRepository.findById(accountId)).thenReturn(Optional.of(account));

        //when
        var ex = assertThrows(IllegalArgumentException.class, () -> withdrawalService.withdraw(accountId, amount));

        //then
        assertAll(
                () -> assertEquals(ex.getMessage(), "Insufficient funds"),
                () -> assertEquals(100, account.getBalance()),
                () -> verify(accountRepository, never()).save(account)
        );
    }
}
