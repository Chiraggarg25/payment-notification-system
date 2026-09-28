package com.paynotify.platform.config;

import com.paynotify.common.client.PlatformProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicConfig {

    /**
     * One topic per configured platform (payment-notifications.udemy, payment-notifications.amazon, ...).
     * KafkaAdmin creates the missing ones on startup, so adding a platform to platforms.yml is enough.
     */
    @Bean
    public KafkaAdmin.NewTopics platformTopics(PlatformProperties platforms) {
        return new KafkaAdmin.NewTopics(platforms.platformIds().stream()
                .map(id -> TopicBuilder.name(platforms.topicFor(id))
                        .partitions(platforms.getPartitionsPerTopic())
                        .replicas(1)
                        .build())
                .toArray(NewTopic[]::new));
    }
}
