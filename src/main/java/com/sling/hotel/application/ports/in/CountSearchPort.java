package com.sling.hotel.application.ports.in;

import com.sling.hotel.domain.model.HotelSearch;

public interface CountSearchPort {

    Result count(String searchId);

    record Result(HotelSearch search, long count) {
    }
}
