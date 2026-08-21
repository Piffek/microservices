-- Skrypt inicjalizacyjny PostgreSQL — tworzy bazy dla poszczególnych serwisów.
-- Uruchamiany automatycznie przez docker-compose przy pierwszym starcie.
--
-- ZASADA: Database-per-Service
-- Każdy serwis ma SWOJĄ bazę — pełna izolacja danych.
-- Serwisy NIE mogą wołać baz innych serwisów (brak shared DB).
-- Jedyna komunikacja: Kafka eventy.

SELECT 'CREATE DATABASE orderdb'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'orderdb')\gexec

SELECT 'CREATE DATABASE inventorydb'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'inventorydb')\gexec
