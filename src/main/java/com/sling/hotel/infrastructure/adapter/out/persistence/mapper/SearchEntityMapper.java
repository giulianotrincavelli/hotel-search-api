package com.sling.hotel.infrastructure.adapter.out.persistence.mapper;

import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.adapter.out.persistence.entity.SearchEntity;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class SearchEntityMapper {

    private SearchEntityMapper() {
    }

    public static HotelSearch toDomain(SearchEntity entity) {
        return new HotelSearch(
                entity.getSearchId(),
                entity.getHotelId(),
                entity.getCheckIn(),
                entity.getCheckOut(),
                stringToAges(entity.getAges())
        );
    }

    public static String agesToString(List<Integer> ages) {
        return ages.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","));
    }

    public static List<Integer> stringToAges(String ages) {
        return Arrays.stream(ages.split(","))
                .map(Integer::parseInt)
                .toList();
    }
}
