package com.paynotify.notification.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paynotify.common.client.PlatformProperties;
import com.paynotify.common.event.PaymentNotificationEvent;
import com.paynotify.notification.delivery.DeliveryOutcome;
import com.paynotify.notification.delivery.NotificationDeliveryService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Message handler shared by every per-platform listener container (see {@link PlatformConsumerContainers}).
 * ack-mode = MANUAL_IMMEDIATE: the offset is committed ONLY after delivery succeeded, or retries / circuit
 * breaker were exhausted and the event was parked in Redis. Anything that throws goes to the bounded
 * error handler (KafkaConsumerConfig), which ends in the dead-letter topic.
 */
@Component
public class PaymentNotificationListener implements AcknowledgingMessageListener<String, String> {

    private static final Logger log = LoggerFactory.getLogger(PaymentNotificationListener.class);

    private final ObjectMapper objectMapper;
    private final NotificationDeliveryService deliveryService;
    private final PlatformProperties platforms;

    public PaymentNotificationListener(ObjectMapper objectMapper,
                                       NotificationDeliveryService deliveryService,
                                       PlatformProperties platforms) {
        this.objectMapper = objectMapper;
        this.deliveryService = deliveryService;
        this.platforms = platforms;
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        if (record == null) {
            return;
        }
        log.info("[KAFKA] received topic={} key={} partition={} offset={}",
                record.topic(), record.key(), record.partition(), record.offset());

        PaymentNotificationEvent event = parseAndValidate(record);
        DeliveryOutcome outcome = deliveryService.deliver(event);

        if (ack != null) {
            ack.acknowledge();
        }
        log.info("[KAFKA] offset {}-{}@{} committed (outcome={})",
                record.topic(), record.partition(), record.offset(), outcome);
    }

    private PaymentNotificationEvent parseAndValidate(ConsumerRecord<String, String> record) {
        String value = record.value();
        if (value == null || value.isBlank()) {
            throw new InvalidEventException("Empty record value (tombstone?)", null);
        }
        PaymentNotificationEvent event;
        try {
            event = objectMapper.readValue(value, PaymentNotificationEvent.class);
        } catch (JsonProcessingException e) {
            throw new InvalidEventException("Un-parseable JSON: " + e.getOriginalMessage(), e);
        }
        if (event == null || isBlank(event.eventId()) || isBlank(event.platformId())
                || isBlank(event.transactionId()) || event.paymentStatus() == null) {
            throw new InvalidEventException("Missing mandatory field(s): " + event, null);
        }
        if (!platforms.isSupported(event.platformId())) {
            throw new InvalidEventException("Unknown platform '" + event.platformId() + "'", null);
        }
        if (!platforms.topicFor(event.platformId()).equals(record.topic())) {
            throw new InvalidEventException("Event for '" + event.platformId() + "' arrived on topic "
                    + record.topic(), null);
        }
        return event;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
