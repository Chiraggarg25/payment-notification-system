package com.paynotify.sweeper;

import com.paynotify.common.config.DeliveryCommonConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
@Import(DeliveryCommonConfig.class)
public class SweeperServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(SweeperServiceApplication.class, args);
    }
}
