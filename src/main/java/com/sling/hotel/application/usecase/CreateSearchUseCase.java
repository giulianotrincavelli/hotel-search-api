package com.sling.hotel.application.usecase;

import com.sling.hotel.application.model.CreateSearchInput;
import com.sling.hotel.application.ports.in.CreateSearchPort;
import com.sling.hotel.application.ports.out.SearchPublisherPort;
import com.sling.hotel.domain.model.HotelSearch;

import java.util.UUID;

public class CreateSearchUseCase implements CreateSearchPort {

    private final SearchPublisherPort searchPublisher;

    public CreateSearchUseCase(SearchPublisherPort searchPublisher) {
        this.searchPublisher = searchPublisher;
    }

    @Override
    public HotelSearch create(CreateSearchInput input) {
        HotelSearch search = new HotelSearch(
                UUID.randomUUID().toString(),
                input.hotelId(),
                input.checkIn(),
                input.checkOut(),
                input.ages()
        );
        searchPublisher.publish(search);
        return search;
    }
}
