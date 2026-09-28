package com.paynotify.common.config;

import com.paynotify.common.client.PlatformClient;
import com.paynotify.common.client.PlatformProperties;
import com.paynotify.common.store.NotificationStore;
import com.paynotify.common.store.RedisStoreProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.client.RestClient;


/**
 * Beans shared by notification-service and sweeper-service. Imported explicitly with
 * {@code @Import(DeliveryCommonConfig.class)} (the common package is not component-scanned).
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({PlatformProperties.class, RedisStoreProperties.class})
public class DeliveryCommonConfig {

    @Bean
    public PlatformClient platformClient(RestClient.Builder builder, PlatformProperties properties) {
        return new PlatformClient(builder, properties);
    }

    @Bean
    public NotificationStore notificationStore(StringRedisTemplate redis, RedisStoreProperties storeProperties) {
        return new NotificationStore(redis, storeProperties);
    }
}
