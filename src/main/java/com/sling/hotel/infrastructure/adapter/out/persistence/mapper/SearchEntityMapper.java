package com.sling.hotel.infrastructure.adapter.out.persistence.mapper;


import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class SearchEntityMapper {

    private SearchEntityMapper() {
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
