package com.paynotify.common.client;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Registry of supported platforms, the webhook URL of each one and the Kafka topic naming.
 * Values live in common/src/main/resources/platforms.yml and are imported by every service.
 */
@ConfigurationProperties(prefix = "platforms")
public class PlatformProperties {

    /** platformId -> webhook URL that receives payment status notifications. */
    private Map<String, String> endpoints = new LinkedHashMap<>();
    private Duration connectTimeout = Duration.ofSeconds(1);
    private Duration readTimeout = Duration.ofSeconds(2);
    /** Every platform gets its own Kafka topic: {topicPrefix}{platformId}, e.g. payment-notifications.udemy */
    private String topicPrefix = "payment-notifications.";
    /**
     * Partitions per platform topic. The message key is the platformId, so all records of a topic hash to the
     * same partition anyway - 1 partition is enough and keeps strict per-platform ordering.
     */
    private int partitionsPerTopic = 1;
    /** Records the consumer could not process (bounded retries exhausted / invalid) go to {topic}{dltSuffix}. */
    private String dltSuffix = ".DLT";

    public String topicFor(String platformId) {
        String id = normalize(platformId);
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("platformId must not be blank");
        }
        return topicPrefix + id;
    }

    public String dltTopicFor(String platformId) {
        return topicFor(platformId) + dltSuffix;
    }

    public List<String> platformIds() {
        return List.copyOf(endpoints.keySet());
    }

    public static String normalize(String platformId) {
        return platformId == null ? null : platformId.trim().toLowerCase(Locale.ROOT);
    }

    public boolean isSupported(String platformId) {
        String id = normalize(platformId);
        return id != null && endpoints.containsKey(id);
    }

    public Optional<String> endpointFor(String platformId) {
        String id = normalize(platformId);
        return id == null ? Optional.empty() : Optional.ofNullable(endpoints.get(id));
    }

    public Map<String, String> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(Map<String, String> endpoints) {
        // normalise keys once so lookups are case-insensitive and never see null keys
        Map<String, String> normalised = new LinkedHashMap<>();
        if (endpoints != null) {
            endpoints.forEach((k, v) -> {
                if (k != null && v != null && !v.isBlank()) {
                    normalised.put(normalize(k), v.trim());
                }
            });
        }
        this.endpoints = normalised;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = requirePositive(connectTimeout, "platforms.connect-timeout");
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public String getDltSuffix() {
        return dltSuffix;
    }

    public void setDltSuffix(String dltSuffix) {
        this.dltSuffix = dltSuffix;
    }

    public int getPartitionsPerTopic() {
        return partitionsPerTopic;
    }

    public void setPartitionsPerTopic(int partitionsPerTopic) {
        if (partitionsPerTopic < 1) {
            throw new IllegalArgumentException("platforms.partitions-per-topic must be >= 1");
        }
        this.partitionsPerTopic = partitionsPerTopic;
    }

    public Duration getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Duration readTimeout) {
        this.readTimeout = requirePositive(readTimeout, "platforms.read-timeout");
    }

    private static Duration requirePositive(Duration d, String name) {
        if (d == null || d.isNegative() || d.isZero()) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
        return d;
    }
}
