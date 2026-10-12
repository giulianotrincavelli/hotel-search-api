package com.sling.hotel.infrastructure.adapter.in.kafka;

public class InvalidKafkaMessageException extends RuntimeException {

    public InvalidKafkaMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
