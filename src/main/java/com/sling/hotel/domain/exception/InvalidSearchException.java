package com.sling.hotel.domain.exception;

public class InvalidSearchException extends RuntimeException {

    public InvalidSearchException(String message) {
        super(message);
    }
}
