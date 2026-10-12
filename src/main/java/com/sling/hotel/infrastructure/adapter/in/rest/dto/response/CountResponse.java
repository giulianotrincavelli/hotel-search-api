package com.sling.hotel.infrastructure.adapter.in.rest.dto.response;

import io.swagger.v3.oas.annotations.media.Schema;

import java.time.LocalDate;
import java.util.List;

@Schema(description = "Response with search details and identical search count")
public record CountResponse(

        @Schema(description = "Search identifier")
        String searchId,

        @Schema(description = "Search details")
        SearchDetail search,

        @Schema(description = "Number of identical searches persisted", example = "100")
        long count
) {

    @Schema(description = "Hotel search details")
    public record SearchDetail(
            String hotelId,
            @Schema(example = "29/12/2023") LocalDate checkIn,
            @Schema(example = "31/12/2023") LocalDate checkOut,
            @Schema(example = "[30, 29, 1, 3]") List<Integer> ages
    ) {

        public SearchDetail {
            ages = ages == null ? null : List.copyOf(ages);
        }
    }
}
