package com.paynotify.notification.config;

import com.paynotify.common.client.PlatformProperties;
import com.paynotify.notification.consumer.InvalidEventException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    /** Per-platform topics (same definition as platform-service) + one dead-letter topic per platform. */
    @Bean
    public KafkaAdmin.NewTopics platformTopics(PlatformProperties platforms) {
        List<NewTopic> topics = new ArrayList<>();
        for (String id : platforms.platformIds()) {
            topics.add(TopicBuilder.name(platforms.topicFor(id))
                    .partitions(platforms.getPartitionsPerTopic()).replicas(1).build());
            topics.add(TopicBuilder.name(platforms.dltTopicFor(id))
                    .partitions(1).replicas(1).build());
        }
        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    /**
     * Reached only when the listener THROWS - i.e. the event could not even be parked in Redis (Redis down),
     * the transaction lock was unavailable, or the record is invalid.
     * <ul>
     *   <li>Offset is NOT committed; the record is re-seeked and re-processed with exponential backoff.</li>
     *   <li>Bounded: after app.kafka.error-handler.max-elapsed-time the record is published to
     *       {topic}.DLT and its offset committed - no infinite loop, no lost message.</li>
     *   <li>{@link InvalidEventException} is not retried at all: straight to the DLT.</li>
     * </ul>
     * Boot wires this CommonErrorHandler bean into the listener container factory, and therefore into every
     * per-platform container created from it.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<?, ?> kafkaTemplate,
                                                 PlatformProperties platforms,
                                                 NotificationProperties props) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    String dlt = record.topic() + platforms.getDltSuffix();
                    log.error("[DLT] {}-{}@{} -> {} ({})", record.topic(), record.partition(), record.offset(), dlt,
                            ex == null ? "?" : ex.getMessage());
                    return new TopicPartition(dlt, -1);   // -1: let Kafka choose the partition
                });

        NotificationProperties.ErrorHandler cfg = props.kafka().errorHandler();
        ExponentialBackOff backOff = new ExponentialBackOff(cfg.initialInterval().toMillis(), cfg.multiplier());
        backOff.setMaxInterval(cfg.maxInterval().toMillis());
        backOff.setMaxElapsedTime(cfg.maxElapsedTime().toMillis());

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        return handler;
    }
}
