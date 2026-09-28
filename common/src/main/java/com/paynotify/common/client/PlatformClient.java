package com.paynotify.common.client;

import com.paynotify.common.event.PaymentNotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.time.Instant;
import java.util.Objects;

/**
 * Synchronous HTTP call to a platform's webhook. Translates every failure into either a
 * {@link RetryableDeliveryException} or a {@link NonRetryableDeliveryException} so that the
 * retry / circuit-breaker policies can make a decision based on the exception type.
 */
public class PlatformClient {

    private static final Logger log = LoggerFactory.getLogger(PlatformClient.class);

    private final RestClient restClient;
    private final PlatformProperties properties;

    public PlatformClient(RestClient.Builder builder, PlatformProperties properties) {
        Objects.requireNonNull(builder, "builder");
        Objects.requireNonNull(properties, "properties");
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout((int) properties.getConnectTimeout().toMillis());
        requestFactory.setReadTimeout((int) properties.getReadTimeout().toMillis());
        this.restClient = builder.requestFactory(requestFactory).build();
        this.properties = properties;
    }

    /** Payload sent to the platform. */
    public record PlatformNotificationPayload(
            String eventId,
            String transactionId,
            String paymentStatus,
            Instant occurredAt) {
    }

    public void send(PaymentNotificationEvent event) {
        if (event == null || event.eventId() == null || event.platformId() == null
                || event.transactionId() == null || event.paymentStatus() == null) {
            throw new NonRetryableDeliveryException("Incomplete event, cannot deliver: " + event, null);
        }
        String url = properties.endpointFor(event.platformId())
                .orElseThrow(() -> new NonRetryableDeliveryException(
                        "No endpoint configured for platform '" + event.platformId() + "'", null));

        PlatformNotificationPayload payload = new PlatformNotificationPayload(
                event.eventId(), event.transactionId(), event.paymentStatus().name(), event.createdAt());
        try {
            restClient.post()
                    .uri(url)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("Idempotency-Key", event.eventId())
                    .header("X-Platform-Id", event.platformId())
                    .body(payload)
                    .retrieve()
                    .toBodilessEntity();
            log.debug("Platform {} acknowledged txn {}", event.platformId(), event.transactionId());
        } catch (HttpServerErrorException e) {
            throw new RetryableDeliveryException("HTTP " + e.getStatusCode().value() + " from " + event.platformId(), e);
        } catch (HttpClientErrorException e) {
            int status = e.getStatusCode().value();
            if (status == 408 || status == 429) {
                throw new RetryableDeliveryException("HTTP " + status + " from " + event.platformId(), e);
            }
            throw new NonRetryableDeliveryException("HTTP " + status + " (rejected) from " + event.platformId(), e);
        } catch (ResourceAccessException e) {
            throw new RetryableDeliveryException("I/O error calling " + event.platformId() + ": " + rootMessage(e), e);
        } catch (RestClientException e) {
            throw new RetryableDeliveryException("Error calling " + event.platformId() + ": " + rootMessage(e), e);
        } catch (IllegalArgumentException e) {
            // e.g. malformed endpoint URL in configuration - retrying cannot fix it
            throw new NonRetryableDeliveryException("Invalid request for " + event.platformId() + ": " + e.getMessage(), e);
        } catch (RuntimeException e) {
            // anything unexpected is treated as transient so it is retried (bounded) instead of silently lost
            throw new RetryableDeliveryException("Unexpected error calling " + event.platformId() + ": " + rootMessage(e), e);
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable root = t;
        int depth = 0;
        while (root.getCause() != null && root.getCause() != root && depth++ < 20) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + (root.getMessage() != null ? " - " + root.getMessage() : "");
    }
}
