package com.example.notification.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Co jakiś czas próbuje ponownie wysłać maile odłożone przez fallback CB.
 *
 * Celowo pobiera JEDEN email na uruchomienie: jeśli emailService jest nadal
 * niedostępny, sendEmail() i tak trafi we własny fallback i email wróci
 * na koniec kolejki — nie chcemy w jednym ticku bić głową w mur po całej
 * kolejce naraz.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PendingEmailScheduler {

    private final PendingEmailQueue pendingEmailQueue;
    private final EmailClient emailClient;

    @Scheduled(fixedDelay = 15000, initialDelay = 15000)
    public void retryPendingEmails() {
        PendingEmailQueue.PendingEmail email = pendingEmailQueue.poll();
        if (email == null) {
            return;
        }

        log.info("Ponawiam wysyłkę odłożonego maila do {}", email.to());
        emailClient.sendEmail(email.to(), email.subject(), email.body());
    }
}
