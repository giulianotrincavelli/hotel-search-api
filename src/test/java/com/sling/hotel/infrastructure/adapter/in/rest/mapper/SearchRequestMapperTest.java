package com.sling.hotel.infrastructure.adapter.in.rest.mapper;

import com.sling.hotel.application.model.CreateSearchInput;
import com.sling.hotel.application.ports.in.CountSearchPort;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.adapter.in.rest.dto.request.SearchRequest;
import com.sling.hotel.infrastructure.adapter.in.rest.dto.response.CountResponse;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

class SearchRequestMapperTest {

    private static final LocalDate CHECK_IN = LocalDate.of(2023, 12, 29);
    private static final LocalDate CHECK_OUT = LocalDate.of(2023, 12, 31);

    @Test
    void toInput_validRequest_mapsAllFields() {
        var request = new SearchRequest("hotel-1", CHECK_IN, CHECK_OUT, List.of(30, 29));

        CreateSearchInput input = SearchRequestMapper.toInput(request);

        assertAll(
                () -> assertThat(input.hotelId()).isEqualTo("hotel-1"),
                () -> assertThat(input.checkIn()).isEqualTo(CHECK_IN),
                () -> assertThat(input.checkOut()).isEqualTo(CHECK_OUT),
                () -> assertThat(input.ages()).containsExactly(30, 29)
        );
    }

    @Test
    void toInput_preservesAgesOrder() {
        var request = new SearchRequest("hotel-1", CHECK_IN, CHECK_OUT, List.of(3, 29, 30, 1));

        CreateSearchInput input = SearchRequestMapper.toInput(request);

        assertThat(input.ages()).containsExactly(3, 29, 30, 1);
    }

    @Test
    void toCountResponse_mapsAllFields() {
        var search = new HotelSearch("s-1", "hotel-1", CHECK_IN, CHECK_OUT, List.of(30, 29, 1, 3));
        var result = new CountSearchPort.Result(search, 42);

        CountResponse response = SearchRequestMapper.toCountResponse(result);

        assertAll(
                () -> assertThat(response.searchId()).isEqualTo("s-1"),
                () -> assertThat(response.search().hotelId()).isEqualTo("hotel-1"),
                () -> assertThat(response.search().checkIn()).isEqualTo(CHECK_IN),
                () -> assertThat(response.search().checkOut()).isEqualTo(CHECK_OUT),
                () -> assertThat(response.search().ages()).containsExactly(30, 29, 1, 3),
                () -> assertThat(response.count()).isEqualTo(42)
        );
    }

    @Test
    void toCountResponse_agesAreImmutable() {
        var search = new HotelSearch("s-1", "hotel-1", CHECK_IN, CHECK_OUT, List.of(30, 29));
        var result = new CountSearchPort.Result(search, 1);

        CountResponse response = SearchRequestMapper.toCountResponse(result);

        var ages = response.search().ages();
        assertThatThrownBy(() -> ages.add(99))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
