package com.sling.hotel.application.model;

import java.time.LocalDate;
import java.util.List;

public record CreateSearchInput(
        String hotelId,
        LocalDate checkIn,
        LocalDate checkOut,
        List<Integer> ages
) {

    public CreateSearchInput {
        ages = ages == null ? null : List.copyOf(ages);
    }
}
