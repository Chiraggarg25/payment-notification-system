package com.paynotify.platform.api;

import com.paynotify.common.event.PaymentStatus;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record PaymentNotificationRequest(
        @NotBlank(message = "platformId is required") String platformId,
        @NotBlank(message = "transactionId is required") @Size(max = 128) String transactionId,
        @NotNull(message = "paymentStatus is required (SUCCESS, FAILED, PENDING, REFUNDED)") PaymentStatus paymentStatus) {
}
