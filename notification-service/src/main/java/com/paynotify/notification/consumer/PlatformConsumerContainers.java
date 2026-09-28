package com.paynotify.notification.consumer;

import com.paynotify.common.client.PlatformProperties;
import com.paynotify.notification.config.NotificationProperties;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Starts ONE Kafka listener container (and ONE consumer group) PER PLATFORM:
 * <pre>
 *   payment-notifications.udemy     -> group notification-service.udemy     -> thread consumer-udemy-0-C-1
 *   payment-notifications.amazon    -> group notification-service.amazon    -> thread consumer-amazon-0-C-1
 *   payment-notifications.coursera  -> group notification-service.coursera  -> thread consumer-coursera-0-C-1
 * </pre>
 * Each platform therefore has its own thread, its own offsets and its own rebalances: while udemy's consumer
 * is blocked in retries/backoff, amazon's consumer keeps delivering. Containers are built from the Boot
 * auto-configured factory, so they inherit ack-mode=manual_immediate and the DefaultErrorHandler.
 */
@Component
public class PlatformConsumerContainers implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(PlatformConsumerContainers.class);

    private final ConcurrentKafkaListenerContainerFactory<String, String> factory;
    private final PlatformProperties platforms;
    private final PaymentNotificationListener listener;
    private final String groupIdPrefix;
    private final String clientIdPrefix;
    /** Guarded by 'this' (start/stop/views are synchronized). */
    private final Map<String, ConcurrentMessageListenerContainer<String, String>> containers = new LinkedHashMap<>();
    private volatile boolean running;

    @SuppressWarnings("unchecked")
    public PlatformConsumerContainers(ConcurrentKafkaListenerContainerFactory<?, ?> factory,
                                      PlatformProperties platforms,
                                      PaymentNotificationListener listener,
                                      NotificationProperties properties) {
        this.factory = (ConcurrentKafkaListenerContainerFactory<String, String>) factory;
        this.platforms = platforms;
        this.listener = listener;
        this.groupIdPrefix = properties.kafka().groupIdPrefix();
        this.clientIdPrefix = properties.kafka().clientIdPrefix();
    }

    public record ConsumerView(String platformId, String topic, String groupId, boolean running,
                               List<String> assignedPartitions) {
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        for (String platformId : platforms.platformIds()) {
            String topic = platforms.topicFor(platformId);
            String groupId = groupIdPrefix + platformId;

            ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(topic);
            container.getContainerProperties().setGroupId(groupId);
            container.getContainerProperties().setClientId(clientIdPrefix + platformId);
            container.getContainerProperties().setMessageListener(listener);
            container.setConcurrency(platforms.getPartitionsPerTopic());
            container.setBeanName("consumer-" + platformId);
            try {
                container.start();
            } catch (RuntimeException e) {
                // fail fast, but never leave half of the consumers running
                log.error("[KAFKA] could not start consumer for '{}', stopping the others", platformId, e);
                stop();
                throw e;
            }
            containers.put(platformId, container);
            log.info("[KAFKA] started consumer for platform '{}' topic={} group={}", platformId, topic, groupId);
        }
        running = true;
    }

    @Override
    public synchronized void stop() {
        containers.forEach((platformId, container) -> {
            try {
                container.stop();
            } catch (RuntimeException e) {
                log.warn("[KAFKA] error stopping consumer for '{}': {}", platformId, e.getMessage());
            }
        });
        containers.clear();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Start after the rest of the context (same phase Spring Kafka uses for @KafkaListener containers). */
    @Override
    public int getPhase() {
        return AbstractMessageListenerContainer.DEFAULT_PHASE;
    }

    public synchronized List<ConsumerView> views() {
        List<ConsumerView> views = new ArrayList<>();
        containers.forEach((platformId, c) -> {
            Collection<TopicPartition> assigned = c.getAssignedPartitions();
            views.add(new ConsumerView(platformId, platforms.topicFor(platformId),
                    c.getContainerProperties().getGroupId(), c.isRunning(),
                    assigned == null ? List.of() : assigned.stream().map(TopicPartition::toString).toList()));
        });
        return views;
    }
}
