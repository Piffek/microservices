package com.example.inventory.support;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import static org.awaitility.Awaitility.await;

/**
 * Testowy konsument Kafki — odczytuje to, co aplikacja faktycznie wysłała na topic.
 *
 * Każda instancja ma własną, losową consumer group i czyta od początku topicu (earliest),
 * więc nie gubi wiadomości wysłanych zanim dostała przydział partycji. Wiadomości z innych
 * testów są odfiltrowywane po kluczu/predykacie — testy nie zależą od kolejności wykonania.
 */
public final class KafkaTestConsumer implements AutoCloseable {

    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(30);

    private final KafkaConsumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> received = new ArrayList<>();

    private KafkaTestConsumer(String bootstrapServers, String topic) {
        this.consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
        this.consumer.subscribe(List.of(topic));
    }

    public static KafkaTestConsumer subscribe(String bootstrapServers, String topic) {
        return new KafkaTestConsumer(bootstrapServers, topic);
    }

    public ConsumerRecord<String, String> awaitSingleRecordWithKey(String key) {
        return awaitRecords(record -> key.equals(record.key()), 1).get(0);
    }

    public List<ConsumerRecord<String, String>> awaitRecords(Predicate<ConsumerRecord<String, String>> filter,
                                                             int expectedCount) {
        await().atMost(AWAIT_TIMEOUT)
                .pollInterval(Duration.ZERO)
                .until(() -> {
                    consumer.poll(POLL_TIMEOUT).forEach(received::add);
                    return matching(filter).size() >= expectedCount;
                });
        return matching(filter);
    }

    private List<ConsumerRecord<String, String>> matching(Predicate<ConsumerRecord<String, String>> filter) {
        return received.stream().filter(filter).toList();
    }

    @Override
    public void close() {
        consumer.close();
    }
}
