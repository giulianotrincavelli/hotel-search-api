package com.sling.hotel.domain.exception;

public class SearchNotFoundException extends RuntimeException {

    private final String searchId;

    public SearchNotFoundException(String searchId) {
        super("Search not found: " + searchId);
        this.searchId = searchId;
    }

    public String getSearchId() {
        return searchId;
    }
}
