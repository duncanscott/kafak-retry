package org.duncanscott.kafka.retry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class KafkaRetry<K, V> implements Closeable {

    public static final String RETRY_COUNT = "retry-count";
    public static final String QUEUE_START = "queue-start";
    private static final Logger logger = LoggerFactory.getLogger(KafkaRetry.class);
    private final String bootstrapServers;
    private final KafkaConsumer<K, V> consumer;
    private final KafkaProducer<K, V> producer;
    private final KafkaRecordHandler<K, V> recordHandler;
    private final String errorTopic;
    private final String dlqTopic;
    private final String autoOffsetResetConfig;
    private final Duration pollTimeout;

    private final Class<? extends Deserializer<K>> keyDeserializer;
    private final Class<? extends Deserializer<V>> valueDeserializer;
    private final Class<? extends Serializer<K>> keySerializer;
    private final Class<? extends Serializer<V>> valueSerializer;

    private final Duration expiration;

    private boolean closed = false;


    //valid values for autoOffsetResetConfig are "none", "earliest", "latest"
    public KafkaRetry(String bootstrapServers, KafkaRecordHandler<K, V> recordHandler, String errorTopic, String dlqTopic,
                      Duration expiration,
                      Class<? extends Deserializer<K>> keyDeserializer, Class<? extends Deserializer<V>> valueDeserializer,
                      Class<? extends Serializer<K>> keySerializer, Class<? extends Serializer<V>> valueSerializer,
                      String autoOffsetResetConfig, Duration pollTimeout) {
        this.bootstrapServers = bootstrapServers;
        this.recordHandler = recordHandler;
        this.errorTopic = errorTopic;
        this.dlqTopic = dlqTopic;
        this.expiration = expiration;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.autoOffsetResetConfig = autoOffsetResetConfig;
        this.pollTimeout = pollTimeout;

        this.producer = producer();
        this.consumer = consumer();
    }

    private static Header headerForKey(ConsumerRecord<?, ?> record, String key) {
        for (Header header : record.headers()) {
            if (header.key().equals(key)) {
                return header;
            }
        }
        return null;
    }

    private static Instant queueStartFromHeader(ConsumerRecord<?, ?> record) {
        Header queueStartHeader = headerForKey(record, QUEUE_START);
        if (queueStartHeader == null) {
            return Instant.now();
        }
        String queueStart = new String(queueStartHeader.value(), StandardCharsets.UTF_8);
        return !queueStart.isEmpty() ? Instant.parse(queueStart) : Instant.now();
    }

    private static int retryCountFromHeader(ConsumerRecord<?, ?> record) {
        Header retryCountHeader = headerForKey(record, RETRY_COUNT);
        if (retryCountHeader == null) {
            return 0;
        }
        String retryCount = new String(retryCountHeader.value(), StandardCharsets.UTF_8).replaceAll("\\D", "");
        return !retryCount.isEmpty() ? Integer.parseInt(retryCount) : 0;
    }

    public void handleRecord(ConsumerRecord<K, V> record) {
        checkClosed();
        try {
            logger.debug("consumed message with key {}", record.key());
            recordHandler.handleRecord(record);
        } catch (Exception e) {
            logger.error("error consuming message with key {}", record.key(), e);
            produceError(record);
        }
    }

    public void produceError(ConsumerRecord<K, V> record) {
        checkClosed();
        producer.send(producerRecord(record, 0));
    }

    public synchronized void retryFailedMessages() {
        checkClosed();
        int recordCount = 0;
        int errorCount = 0;
        int successCount = 0;
        Instant startTime = Instant.now();
        logger.info("polling for errors; start-time: {}", startTime);
        for (ConsumerRecord<K, V> record : consumer.poll(pollTimeout)) {
            try {
                recordCount++;
                logger.info("retry record {} with key {}", recordCount, record.key());
                recordHandler.handleRecord(record);
                successCount++;
            } catch (Exception e) {
                try {
                    errorCount++;
                    produceErrorForRetry(record, e);
                } catch (Exception e2) {
                    logger.error("error producing error for retry", e2);
                }
            } finally {
                consumer.commitSync(offsets(record));
            }
        }
        Instant endTime = Instant.now();
        Duration elapsed = Duration.between(startTime, endTime);
        logger.info("processed {} errors; start-time: {}, end-time: {}, elapsed: {}, success-count: {}, remaining-errors: {}", recordCount, startTime, endTime, elapsed, successCount, errorCount);
    }

    private Map<TopicPartition, OffsetAndMetadata> offsets(ConsumerRecord<K, V> record) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1);
        offsets.put(topicPartition, offsetAndMetadata);
        return offsets;
    }

    private void produceErrorForRetry(ConsumerRecord<K, V> record, Exception e) {
        int retryCount = retryCountFromHeader(record) + 1;
        logger.error("error re-trying record with key {}, retry-count {}", record.key(), retryCount, e);
        producer.send(producerRecord(record, retryCount));
    }

    private void checkClosed() {
        if (closed) {
            throw new RuntimeException("closed");
        }
    }

    private KafkaConsumer<K, V> consumer() {
        KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProperties());
        consumer.subscribe(Collections.singletonList(errorTopic));
        return consumer;
    }

    private KafkaProducer<K, V> producer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
        return new KafkaProducer<>(properties);
    }

    private Properties consumerProperties() {
        Properties consumerProperties = new Properties();
        String groupIdConfigBuilder = "retry-queue_" + errorTopic;
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfigBuilder);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return consumerProperties;
    }

    private ProducerRecord<K, V> producerRecord(ConsumerRecord<K, V> consumerRecord, int retryCount) {
        Instant queueStart = queueStartFromHeader(consumerRecord);
        List<Header> headers = new ArrayList<Header>();
        headers.add(new RecordHeader(RETRY_COUNT, String.valueOf(retryCount).getBytes()));
        headers.add(new RecordHeader(QUEUE_START, queueStart.toString().getBytes()));
        Integer partition = null;
        Instant recordTime = Instant.ofEpochMilli(consumerRecord.timestamp());
        Instant expirationTime = queueStart.plus(expiration);
        boolean isExpired = recordTime.isAfter(expirationTime);
        if (isExpired) {
            logger.warn("sending record with key {}, queue-start {}, retry-count {} to dead-letter-queue {}: record has expired", consumerRecord.key(), queueStart, retryCount, dlqTopic);
        } else {
            logger.info("sending record with key {}, queue-start {}, retry-count {} to error-queue {}", consumerRecord.key(), queueStart, retryCount, errorTopic);
        }
        return new ProducerRecord<K, V>(isExpired ? dlqTopic : errorTopic, partition, System.currentTimeMillis(), consumerRecord.key(), consumerRecord.value(), headers);
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            logger.info("closing");
            try {
                consumer.close();
            } finally {
                producer.close();
            }
        } finally {
            this.closed = true;
        }
    }

}
