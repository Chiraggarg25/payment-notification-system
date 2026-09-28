package com.paynotify.platform.api;

import com.paynotify.platform.kafka.PublishFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.ErrorResponse;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;

import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail validation(MethodArgumentNotValidException e) {
        String detail = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail unreadable(HttpMessageNotReadableException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "Malformed request body (paymentStatus must be one of SUCCESS, FAILED, PENDING, REFUNDED)");
    }

    @ExceptionHandler({UnsupportedPlatformException.class, InvalidIdempotencyKeyException.class})
    public ProblemDetail badRequest(RuntimeException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(PublishFailedException.class)
    public ProblemDetail publishFailed(PublishFailedException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
    }

    /** Failures of the async publish arrive wrapped; unwrap and map them like the synchronous ones. */
    @ExceptionHandler(CompletionException.class)
    public ProblemDetail completion(CompletionException e) {
        if (e.getCause() instanceof PublishFailedException pfe) {
            return publishFailed(pfe);
        }
        return unexpected(e);
    }

    @ExceptionHandler(AsyncRequestTimeoutException.class)
    public ProblemDetail asyncTimeout(AsyncRequestTimeoutException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "Timed out waiting for Kafka");
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail unexpected(Exception e) {
        if (e instanceof ErrorResponse errorResponse) {
            // standard Spring MVC exceptions (405, 415, missing header ...) keep their proper status
            return errorResponse.getBody();
        }
        log.error("Unexpected error", e);
        return ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "Unexpected error");
    }
}
