package com.sling.hotel.application.ports.in;

import com.sling.hotel.domain.model.HotelSearch;

public interface SaveSearchPort {

    void save(HotelSearch search);
}
