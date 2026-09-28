package com.paynotify.platform.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paynotify.common.client.PlatformProperties;
import com.paynotify.common.event.PaymentNotificationEvent;
import com.paynotify.platform.config.PlatformServiceProperties;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class PaymentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final PlatformProperties platforms;
    private final PlatformServiceProperties props;

    public PaymentEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                 ObjectMapper objectMapper,
                                 PlatformProperties platforms,
                                 PlatformServiceProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.platforms = platforms;
        this.props = props;
    }

    /**
     * Non-blocking publish: returns a future completed by the Kafka producer's I/O thread once the broker
     * acknowledged (acks=all), or failed with {@link PublishFailedException} (error or timeout).
     * <ul>
     *   <li>TOPIC = payment-notifications.{platformId}: every platform has its own topic and consumer.</li>
     *   <li>KEY = platformId: all events of the platform land on the same partition -> strict ordering.</li>
     * </ul>
     * The continuation only builds a log line; nothing blocking runs on the producer thread.
     */
    public CompletableFuture<RecordMetadata> publish(PaymentNotificationEvent event) {
        String payload;
        String topic;
        try {
            payload = objectMapper.writeValueAsString(event);
            topic = platforms.topicFor(event.platformId());
        } catch (JsonProcessingException | IllegalArgumentException e) {
            return CompletableFuture.failedFuture(new PublishFailedException("Could not build Kafka record: " + e.getMessage(), e));
        }

        CompletableFuture<RecordMetadata> result;
        try {
            // send() itself may block up to max.block.ms (bounded in application.yml) when metadata is unavailable
            result = kafkaTemplate.send(topic, event.platformId(), payload)
                    .thenApply(sendResult -> sendResult.getRecordMetadata());
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(new PublishFailedException("Kafka publish failed: " + e.getMessage(), e));
        }

        return result
                .orTimeout(props.publishTimeout().toMillis(), TimeUnit.MILLISECONDS)
                .handle((md, ex) -> {
                    if (ex != null) {
                        Throwable cause = ex instanceof CompletionException && ex.getCause() != null ? ex.getCause() : ex;
                        String reason = cause instanceof TimeoutException
                                ? "no broker ack within " + props.publishTimeout()
                                : cause.getMessage();
                        log.error("[KAFKA] publish failed event={} topic={}: {}", event.eventId(), topic, reason);
                        throw new CompletionException(new PublishFailedException("Kafka publish failed: " + reason, cause));
                    }
                    if (md == null) {
                        throw new CompletionException(new PublishFailedException("Kafka returned no metadata", null));
                    }
                    log.info("[KAFKA] published event={} key={} txn={} status={} -> {}-{}@{}",
                            event.eventId(), event.platformId(), event.transactionId(), event.paymentStatus(),
                            md.topic(), md.partition(), md.offset());
                    return md;
                });
    }
}
