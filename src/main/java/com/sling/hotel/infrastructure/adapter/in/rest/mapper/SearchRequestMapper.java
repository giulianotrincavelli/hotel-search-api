package com.sling.hotel.infrastructure.adapter.in.rest.mapper;

import com.sling.hotel.application.ports.in.CountSearchPort;
import com.sling.hotel.application.model.CreateSearchInput;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.adapter.in.rest.dto.request.SearchRequest;
import com.sling.hotel.infrastructure.adapter.in.rest.dto.response.CountResponse;

public final class SearchRequestMapper {

    private SearchRequestMapper() {
    }

    public static CreateSearchInput toInput(SearchRequest request) {
        return new CreateSearchInput(request.hotelId(), request.checkIn(), request.checkOut(), request.ages());
    }

    public static CountResponse toCountResponse(CountSearchPort.Result result) {
        HotelSearch search = result.search();
        var detail = new CountResponse.SearchDetail(
                search.hotelId(),
                search.checkIn(),
                search.checkOut(),
                search.ages()
        );
        return new CountResponse(search.searchId(), detail, result.count());
    }
}
