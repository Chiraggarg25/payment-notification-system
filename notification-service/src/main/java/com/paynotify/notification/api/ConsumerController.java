package com.paynotify.notification.api;

import com.paynotify.notification.consumer.PlatformConsumerContainers;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** Shows the per-platform Kafka consumers (topic, consumer group, assigned partitions). */
@RestController
public class ConsumerController {

    private final PlatformConsumerContainers containers;

    public ConsumerController(PlatformConsumerContainers containers) {
        this.containers = containers;
    }

    @GetMapping("/api/consumers")
    public List<PlatformConsumerContainers.ConsumerView> consumers() {
        return containers.views();
    }
}
