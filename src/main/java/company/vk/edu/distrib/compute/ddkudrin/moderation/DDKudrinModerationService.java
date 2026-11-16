package company.vk.edu.distrib.compute.ddkudrin.moderation;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import company.vk.edu.distrib.compute.moderation.ModerationService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DDKudrinModerationService implements ModerationService {
    private static final Logger LOG = LoggerFactory.getLogger(DDKudrinModerationService.class);
    private final String bootstrapServers;
    private final DDKudrinModerator moderator;
    private final AtomicBoolean running = new AtomicBoolean();
    private KafkaConsumer<String, String> consumer;
    private Thread worker;

    public DDKudrinModerationService(String bootstrapServers) throws IOException {
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers);
        moderator = new DDKudrinModerator();
    }

    @Override
    public synchronized void start() {
        if (worker != null) {
            throw new IllegalStateException("Moderation service already started");
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ddkudrin-moderation");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("links.created"));
        running.set(true);
        worker = new Thread(this::run, "ddkudrin-moderation");
        worker.start();
    }

    private void run() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);
        try (KafkaConsumer<String, String> ignored = consumer;
             KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (running.get()) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (!running.get()) {
                        break;
                    }
                    ProducerRecord<String, String> output = moderator.moderate(record);
                    try {
                        producer.send(output).get(10, TimeUnit.SECONDS);
                    } catch (KafkaException | ExecutionException | TimeoutException e) {
                        LOG.warn("Cannot publish moderation result; retrying", e);
                        consumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                        break;
                    }
                    consumer.commitSync(Map.of(new TopicPartition(record.topic(), record.partition()),
                        new org.apache.kafka.clients.consumer.OffsetAndMetadata(record.offset() + 1)));
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                LOG.error("Moderation consumer stopped unexpectedly", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            LOG.error("Moderation service stopped unexpectedly", e);
        }
    }

    @Override
    public synchronized void stop() {
        running.set(false);
        if (worker == null) {
            return;
        }
        consumer.wakeup();
        try {
            worker.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping moderation", e);
        }
    }
}

