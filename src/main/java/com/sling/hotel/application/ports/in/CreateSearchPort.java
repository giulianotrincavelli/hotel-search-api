package com.sling.hotel.application.ports.in;

import com.sling.hotel.application.model.CreateSearchInput;
import com.sling.hotel.domain.model.HotelSearch;

public interface CreateSearchPort {

    HotelSearch create(CreateSearchInput input);
}
