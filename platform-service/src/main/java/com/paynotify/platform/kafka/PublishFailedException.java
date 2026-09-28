package com.paynotify.platform.kafka;

public class PublishFailedException extends RuntimeException {
    public PublishFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
