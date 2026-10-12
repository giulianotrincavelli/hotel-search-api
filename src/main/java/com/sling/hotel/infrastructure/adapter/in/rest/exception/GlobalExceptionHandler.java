package com.sling.hotel.infrastructure.adapter.in.rest.exception;

import com.sling.hotel.application.exception.PublishException;
import com.sling.hotel.domain.exception.InvalidSearchException;
import com.sling.hotel.domain.exception.SearchNotFoundException;
import com.sling.hotel.infrastructure.adapter.in.rest.dto.response.ErrorResponse;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import tools.jackson.databind.exc.MismatchedInputException;
import tools.jackson.databind.exc.UnrecognizedPropertyException;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .sorted()
                .toList();

        return ResponseEntity.badRequest()
                .body(new ErrorResponse(400, "Validation failed", errors));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(ConstraintViolationException ex) {
        List<String> errors = ex.getConstraintViolations().stream()
                .map(ConstraintViolation::getMessage)
                .sorted()
                .toList();

        return ResponseEntity.badRequest()
                .body(new ErrorResponse(400, "Validation failed", errors));
    }

    @ExceptionHandler(HandlerMethodValidationException.class)
    public ResponseEntity<ErrorResponse> handleMethodValidation(HandlerMethodValidationException ex) {
        List<String> errors = ex.getAllErrors().stream()
                .map(MessageSourceResolvable::getDefaultMessage)
                .sorted()
                .toList();

        return ResponseEntity.badRequest()
                .body(new ErrorResponse(400, "Validation failed", errors));
    }

    @ExceptionHandler(InvalidSearchException.class)
    public ResponseEntity<ErrorResponse> handleInvalidSearch(InvalidSearchException ex) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(400, ex.getMessage()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadable(HttpMessageNotReadableException ex) {
        for (Throwable cause = ex.getCause(); cause != null; cause = cause.getCause()) {
            if (cause instanceof UnrecognizedPropertyException unknown) {
                return ResponseEntity.badRequest()
                        .body(new ErrorResponse(400, "Unknown property: " + unknown.getPropertyName()));
            }
            if (cause instanceof MismatchedInputException mie) {
                String field = buildFieldPath(mie);
                if (field.isEmpty()) {
                    continue;
                }
                if (isDateTarget(mie)) {
                    return ResponseEntity.badRequest()
                            .body(new ErrorResponse(400, field + " must be a valid date in dd/MM/yyyy format"));
                }
                if (isIntegerTarget(mie)) {
                    return ResponseEntity.badRequest()
                            .body(new ErrorResponse(400, field + " must contain only integer numbers"));
                }
                if (isStringTarget(mie)) {
                    return ResponseEntity.badRequest()
                            .body(new ErrorResponse(400, field + " must be a string"));
                }
            }
        }
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(400, "Malformed request body"));
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ErrorResponse> handleMissingParam(MissingServletRequestParameterException ex) {
        return ResponseEntity.badRequest()
                .body(new ErrorResponse(400, ex.getParameterName() + " is required"));
    }

    @ExceptionHandler(SearchNotFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(SearchNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ErrorResponse(404, ex.getMessage()));
    }

    @ExceptionHandler(PublishException.class)
    public ResponseEntity<ErrorResponse> handlePublishFailure(PublishException ex) {
        log.warn("Message publish failed: {}", ex.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ErrorResponse(503, "Service temporarily unavailable"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleUnexpected(Exception ex) {
        if (ex instanceof org.springframework.web.ErrorResponse mvcError) {
            var status = mvcError.getStatusCode();
            String message = mvcError.getBody().getDetail();
            if (status.is5xxServerError()) {
                log.error("Spring MVC error", ex);
                message = "Internal server error";
            } else if (message == null) {
                message = mvcError.getBody().getTitle();
            }
            return ResponseEntity.status(status)
                    .headers(mvcError.getHeaders())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new ErrorResponse(status.value(), message));
        }
        log.error("Unexpected error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorResponse(500, "Internal server error"));
    }

    private static String buildFieldPath(MismatchedInputException mie) {
        return mie.getPath().stream()
                .map(ref -> {
                    String name = ref.getPropertyName();
                    if (name != null) return name;
                    if (ref.getIndex() >= 0) return "[" + ref.getIndex() + "]";
                    return "";
                })
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining());
    }

    private static boolean isDateTarget(MismatchedInputException mie) {
        Class<?> target = mie.getTargetType();
        return target != null && LocalDate.class.isAssignableFrom(target);
    }

    private static boolean isIntegerTarget(MismatchedInputException mie) {
        Class<?> target = mie.getTargetType();
        return target != null && (Integer.class.isAssignableFrom(target) || int.class == target);
    }

    private static boolean isStringTarget(MismatchedInputException mie) {
        Class<?> target = mie.getTargetType();
        return target != null && String.class.isAssignableFrom(target);
    }
}
