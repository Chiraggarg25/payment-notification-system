package com.paynotify.platform.api;

public class UnsupportedPlatformException extends RuntimeException {
    public UnsupportedPlatformException(String platformId) {
        super("Unsupported platformId '" + platformId + "'");
    }
}
