package com.sling.hotel.domain.model;

import com.sling.hotel.domain.exception.InvalidSearchException;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;

class HotelSearchTest {

    private static final String SEARCH_ID = "search-001";
    private static final String HOTEL_ID = "1234aBc";
    private static final LocalDate CHECK_IN = LocalDate.of(2023, 12, 29);
    private static final LocalDate CHECK_OUT = LocalDate.of(2023, 12, 31);
    private static final List<Integer> AGES = List.of(30, 29, 1, 3);

    @Test
    void shouldCreateValidHotelSearch() {
        var search = new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, AGES);

        assertAll(
                () -> assertThat(search.searchId()).isEqualTo(SEARCH_ID),
                () -> assertThat(search.hotelId()).isEqualTo(HOTEL_ID),
                () -> assertThat(search.checkIn()).isEqualTo(CHECK_IN),
                () -> assertThat(search.checkOut()).isEqualTo(CHECK_OUT),
                () -> assertThat(search.ages()).containsExactly(30, 29, 1, 3)
        );
    }

    @Test
    void shouldPerformDefensiveCopyOfAges() {
        var mutableAges = new ArrayList<>(AGES);
        var search = new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, mutableAges);

        mutableAges.add(99);

        assertThat(search.ages()).containsExactly(30, 29, 1, 3);
    }

    @Test
    void shouldReturnUnmodifiableAgesList() {
        var search = new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, AGES);

        assertThatThrownBy(() -> search.ages().add(99))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldBeIndependentFromOriginalList() {
        var original = new ArrayList<>(AGES);
        var search = new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, original);

        original.set(0, 999);

        assertThat(search.ages().getFirst()).isEqualTo(30);
    }

    @Test
    void shouldRejectNullSearchId() {
        assertThatThrownBy(() -> new HotelSearch(null, HOTEL_ID, CHECK_IN, CHECK_OUT, AGES))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("searchId");
    }

    @Test
    void shouldRejectBlankSearchId() {
        assertThatThrownBy(() -> new HotelSearch("  ", HOTEL_ID, CHECK_IN, CHECK_OUT, AGES))
                .isInstanceOf(InvalidSearchException.class)
                .hasMessageContaining("searchId");
    }

    @Test
    void shouldRejectNullHotelId() {
        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, null, CHECK_IN, CHECK_OUT, AGES))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("hotelId");
    }

    @Test
    void shouldRejectBlankHotelId() {
        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, "", CHECK_IN, CHECK_OUT, AGES))
                .isInstanceOf(InvalidSearchException.class)
                .hasMessageContaining("hotelId");
    }

    @Test
    void shouldRejectNullCheckIn() {
        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, HOTEL_ID, null, CHECK_OUT, AGES))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("checkIn");
    }

    @Test
    void shouldRejectNullCheckOut() {
        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, null, AGES))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("checkOut");
    }

    @Test
    void shouldRejectCheckOutNotAfterCheckIn() {
        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_IN, AGES))
                .isInstanceOf(InvalidSearchException.class)
                .hasMessageContaining("checkOut must be after checkIn");
    }

    @Test
    void shouldRejectCheckOutBeforeCheckIn() {
        var before = CHECK_IN.minusDays(1);

        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, before, AGES))
                .isInstanceOf(InvalidSearchException.class)
                .hasMessageContaining("checkOut must be after checkIn");
    }

    @Test
    void shouldRejectNullAges() {
        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("ages");
    }

    @Test
    void shouldRejectEmptyAges() {
        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, Collections.emptyList()))
                .isInstanceOf(InvalidSearchException.class)
                .hasMessageContaining("ages must not be empty");
    }

    @Test
    void shouldRejectNegativeAge() {
        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, List.of(30, -1)))
                .isInstanceOf(InvalidSearchException.class)
                .hasMessageContaining("age must be between 0 and 100");
    }

    @Test
    void shouldRejectAgeAbove100() {
        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, List.of(30, 101)))
                .isInstanceOf(InvalidSearchException.class)
                .hasMessageContaining("age must be between 0 and 100");
    }

    @Test
    void shouldAcceptAge100() {
        var search = new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, List.of(100));

        assertThat(search.ages()).containsExactly(100);
    }

    @Test
    void shouldAcceptZeroAge() {
        var search = new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, List.of(0));

        assertThat(search.ages()).containsExactly(0);
    }

    @Test
    void shouldPreserveAgeOrder() {
        var ages1 = List.of(30, 29, 1, 3);
        var ages2 = List.of(3, 29, 30, 1);

        var search1 = new HotelSearch("id1", HOTEL_ID, CHECK_IN, CHECK_OUT, ages1);
        var search2 = new HotelSearch("id2", HOTEL_ID, CHECK_IN, CHECK_OUT, ages2);

        assertThat(search1.ages()).isNotEqualTo(search2.ages());
    }

    @Test
    void shouldAcceptHotelIdOf50Characters() {
        var hotelId = "a".repeat(50);
        var search = new HotelSearch(SEARCH_ID, hotelId, CHECK_IN, CHECK_OUT, AGES);
        assertThat(search.hotelId()).hasSize(50);
    }

    @Test
    void shouldRejectHotelIdOf51Characters() {
        var hotelId = "a".repeat(51);
        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, hotelId, CHECK_IN, CHECK_OUT, AGES))
                .isInstanceOf(InvalidSearchException.class)
                .hasMessageContaining("hotelId must have at most 50 characters");
    }

    @Test
    void shouldReject51Ages() {
        var ages = Collections.nCopies(51, 50);
        assertThatThrownBy(() -> new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, ages))
                .isInstanceOf(InvalidSearchException.class)
                .hasMessageContaining("ages must have at most 50 elements");
    }

    @Test
    void shouldAcceptRepeatedAges() {
        var search = new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, List.of(5, 5, 5, 5));
        assertThat(search.ages()).containsExactly(5, 5, 5, 5);
    }

    @Test
    void shouldAccept50AgesOf100() {
        var ages = Collections.nCopies(50, 100);
        var search = new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, ages);
        assertThat(search.ages()).hasSize(50).allMatch(a -> a == 100);
    }

    @Test
    void shouldSupportEqualityBasedOnAllFields() {
        var search1 = new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, AGES);
        var search2 = new HotelSearch(SEARCH_ID, HOTEL_ID, CHECK_IN, CHECK_OUT, List.of(30, 29, 1, 3));

        assertAll(
                () -> assertThat(search1).isEqualTo(search2),
                () -> assertThat(search1.hashCode()).isEqualTo(search2.hashCode())
        );
    }

    @Test
    void shouldNotBeEqualWithDifferentSearchId() {
        var search1 = new HotelSearch("id1", HOTEL_ID, CHECK_IN, CHECK_OUT, AGES);
        var search2 = new HotelSearch("id2", HOTEL_ID, CHECK_IN, CHECK_OUT, AGES);

        assertThat(search1).isNotEqualTo(search2);
    }
}
