package org.duncanscott.kafka.retry;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;

public class KafkaRetryStringSerdes extends KafkaRetry<String, String> {

    private static final Duration defaultPollTimeout = Duration.ofMillis(100L);
    private static final String defaultAutoOffsetResetConfig = "none";
    private static final Class<StringDeserializer> keyDeserializer = StringDeserializer.class;
    private static final Class<StringDeserializer> valueDeserializer = StringDeserializer.class;
    private static final Class<StringSerializer> keySerializer = StringSerializer.class;
    private static final Class<StringSerializer> valueSerializer = StringSerializer.class;


    public KafkaRetryStringSerdes(String bootstrapServers, KafkaRecordHandler<String, String> recordHandler,
                                  String errorTopic, String dlqTopic,
                                  Duration expiration,
                                  String autoOffsetResetConfig, Duration pollTimeout) {
        super(bootstrapServers, recordHandler, errorTopic, dlqTopic, expiration, keyDeserializer, valueDeserializer, keySerializer, valueSerializer, autoOffsetResetConfig, pollTimeout);
    }

    public KafkaRetryStringSerdes(String bootstrapServers, KafkaRecordHandler<String, String> recordHandler,
                                  String errorTopic, String dlqTopic,
                                  Duration expiration,
                                  String autoOffsetResetConfig) {
        super(bootstrapServers, recordHandler, errorTopic, dlqTopic, expiration, keyDeserializer, valueDeserializer, keySerializer, valueSerializer, autoOffsetResetConfig, defaultPollTimeout);
    }

    public KafkaRetryStringSerdes(String bootstrapServers, KafkaRecordHandler<String, String> recordHandler,
                                  String errorTopic, String dlqTopic,
                                  Duration expiration) {
        super(bootstrapServers, recordHandler, errorTopic, dlqTopic, expiration, keyDeserializer, valueDeserializer, keySerializer, valueSerializer, defaultAutoOffsetResetConfig, defaultPollTimeout);

    }
}
