package com.sling.hotel.application.ports.out;

import com.sling.hotel.domain.model.HotelSearch;

import java.util.Optional;

public interface SearchRepositoryPort {

    void save(HotelSearch search);

    Optional<HotelSearch> findById(String searchId);

    long countIdentical(HotelSearch search);
}
