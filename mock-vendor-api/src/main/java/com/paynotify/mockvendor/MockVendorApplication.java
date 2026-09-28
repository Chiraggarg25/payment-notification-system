package com.paynotify.mockvendor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MockVendorApplication {
    public static void main(String[] args) {
        SpringApplication.run(MockVendorApplication.class, args);
    }
}
