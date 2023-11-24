package org.duncanscott.kafka.retry;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkaRecordHandler<K, V> {
    void handleRecord(ConsumerRecord<K, V> record) throws Exception;
}
