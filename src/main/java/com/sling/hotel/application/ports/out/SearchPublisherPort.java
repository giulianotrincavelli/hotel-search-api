package com.sling.hotel.application.ports.out;

import com.sling.hotel.domain.model.HotelSearch;

public interface SearchPublisherPort {

    void publish(HotelSearch search);
}
