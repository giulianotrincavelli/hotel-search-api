package com.sling.hotel.domain.model;

import com.sling.hotel.domain.exception.InvalidSearchException;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

// Age order matters: [30,29,1,3] ≠ [3,29,30,1]. Each search has a unique searchId.
public record HotelSearch(
        String searchId,
        String hotelId,
        LocalDate checkIn,
        LocalDate checkOut,
        List<Integer> ages
) {

    public HotelSearch {
        Objects.requireNonNull(searchId, "searchId must not be null");
        Objects.requireNonNull(hotelId, "hotelId must not be null");
        Objects.requireNonNull(checkIn, "checkIn must not be null");
        Objects.requireNonNull(checkOut, "checkOut must not be null");
        Objects.requireNonNull(ages, "ages must not be null");

        if (searchId.isBlank()) {
            throw new InvalidSearchException("searchId must not be blank");
        }
        if (hotelId.isBlank()) {
            throw new InvalidSearchException("hotelId must not be blank");
        }
        if (hotelId.length() > 50) {
            throw new InvalidSearchException("hotelId must have at most 50 characters");
        }
        if (!checkOut.isAfter(checkIn)) {
            throw new InvalidSearchException("checkOut must be after checkIn");
        }
        if (ages.isEmpty()) {
            throw new InvalidSearchException("ages must not be empty");
        }
        if (ages.size() > 50) {
            throw new InvalidSearchException("ages must have at most 50 elements");
        }
        for (Integer age : ages) {
            Objects.requireNonNull(age, "each age must not be null");
            if (age < 0 || age > 100) {
                throw new InvalidSearchException("each age must be between 0 and 100");
            }
        }

        ages = List.copyOf(ages);
    }
}
