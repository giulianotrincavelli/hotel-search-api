package com.sling.hotel.infrastructure.adapter.out.persistence.mapper;

import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.adapter.out.persistence.entity.SearchEntity;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

class SearchEntityMapperTest {

    @Test
    void shouldMapEntityToDomain() {
        var entity = new SearchEntity(
                "search-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                "30,29,1,3"
        );

        HotelSearch search = SearchEntityMapper.toDomain(entity);

        assertAll(
                () -> assertThat(search.searchId()).isEqualTo("search-1"),
                () -> assertThat(search.hotelId()).isEqualTo("hotel-1"),
                () -> assertThat(search.checkIn()).isEqualTo(LocalDate.of(2023, 12, 29)),
                () -> assertThat(search.checkOut()).isEqualTo(LocalDate.of(2023, 12, 31)),
                () -> assertThat(search.ages()).containsExactly(30, 29, 1, 3)
        );
    }

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
        assertThat(serialized.length()).isEqualTo(199);
        assertThat(serialized.length()).isLessThanOrEqualTo(200);
    }

    @Test
    void shouldHandleSingleAge() {
        String csv = SearchEntityMapper.agesToString(List.of(5));
        assertThat(csv).isEqualTo("5");
        assertThat(SearchEntityMapper.stringToAges(csv)).containsExactly(5);
    }
}
