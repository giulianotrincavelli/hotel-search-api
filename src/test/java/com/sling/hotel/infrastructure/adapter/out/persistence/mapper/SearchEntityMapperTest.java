package com.sling.hotel.infrastructure.adapter.out.persistence.mapper;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

class SearchEntityMapperTest {

    @Test
    void shouldPreserveAgeOrder() {
        assertAll(
                () -> assertThat(SearchEntityMapper.agesToString(List.of(30, 29, 1, 3))).isEqualTo("30,29,1,3"),
                () -> assertThat(SearchEntityMapper.agesToString(List.of(3, 29, 30, 1))).isEqualTo("3,29,30,1")
        );
    }

    @Test
    void shouldRoundTripAgesCorrectly() {
        var ages = List.of(30, 29, 1, 3);
        String serialized = SearchEntityMapper.agesToString(ages);
        List<Integer> deserialized = SearchEntityMapper.stringToAges(serialized);

        assertThat(deserialized).containsExactlyElementsOf(ages);
    }

    @Test
    void differentAgeOrdersShouldProduceDifferentStrings() {
        String ages1 = SearchEntityMapper.agesToString(List.of(30, 29, 1, 3));
        String ages2 = SearchEntityMapper.agesToString(List.of(3, 29, 30, 1));

        assertThat(ages1).isNotEqualTo(ages2);
    }

    @Test
    void worstCaseAgesStringShouldFitWithinColumnLimit() {
        List<Integer> maxAges = Collections.nCopies(50, 100);
        String serialized = SearchEntityMapper.agesToString(maxAges);

        // 50 ages of "100" (3 chars each) + 49 commas = 199 chars — fits in VARCHAR2(200 CHAR)
        assertAll(
                () -> assertThat(serialized).hasSize(199),
                () -> assertThat(serialized.length()).isLessThanOrEqualTo(200));
    }

    @Test
    void shouldHandleSingleAge() {
        String csv = SearchEntityMapper.agesToString(List.of(5));
        assertAll(
                () -> assertThat(csv).isEqualTo("5"),
                () -> assertThat(SearchEntityMapper.stringToAges(csv)).containsExactly(5));
    }
}
