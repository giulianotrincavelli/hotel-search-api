package com.sling.hotel.infrastructure.adapter.out.persistence.repository;

import java.time.LocalDate;

public record SearchCountRow(String searchId, String hotelId, LocalDate checkIn,
                             LocalDate checkOut, String ages, long count) {
}
