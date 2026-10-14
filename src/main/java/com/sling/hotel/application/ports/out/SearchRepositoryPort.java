package com.sling.hotel.application.ports.out;

import com.sling.hotel.domain.model.HotelSearch;

import java.util.Optional;

public interface SearchRepositoryPort {

    void save(HotelSearch search);

    Optional<SearchCount> findWithCount(String searchId);

    record SearchCount(HotelSearch search, long count) {
    }
}
