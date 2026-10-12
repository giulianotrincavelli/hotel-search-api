package com.sling.hotel.domain.exception;

public class IdempotencyConflictException extends RuntimeException {

    private final String searchId;

    public IdempotencyConflictException(String searchId) {
        super("Idempotency conflict: searchId " + searchId + " already exists with different payload");
        this.searchId = searchId;
    }

    public String getSearchId() {
        return searchId;
    }
}
