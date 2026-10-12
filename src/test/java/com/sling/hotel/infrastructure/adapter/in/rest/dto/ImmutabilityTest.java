package com.sling.hotel.infrastructure.adapter.in.rest.dto;

import com.sling.hotel.infrastructure.adapter.in.rest.dto.request.SearchRequest;
import com.sling.hotel.infrastructure.adapter.in.rest.dto.response.CountResponse;
import com.sling.hotel.infrastructure.adapter.in.rest.dto.response.ErrorResponse;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ImmutabilityTest {

    private static final LocalDate CHECK_IN = LocalDate.of(2023, 12, 29);
    private static final LocalDate CHECK_OUT = LocalDate.of(2023, 12, 31);

    @Test
    void searchRequest_agesDefensiveCopy() {
        var mutable = new ArrayList<>(List.of(30, 29));
        var request = new SearchRequest("hotel-1", CHECK_IN, CHECK_OUT, mutable);

        mutable.add(99);

        assertThat(request.ages()).containsExactly(30, 29);
    }

    @Test
    void searchRequest_agesUnmodifiable() {
        var request = new SearchRequest("hotel-1", CHECK_IN, CHECK_OUT, List.of(30, 29));

        assertThatThrownBy(() -> request.ages().add(99))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void countResponseSearchDetail_agesDefensiveCopy() {
        var mutable = new ArrayList<>(List.of(30, 29));
        var detail = new CountResponse.SearchDetail("hotel-1", CHECK_IN, CHECK_OUT, mutable);

        mutable.add(99);

        assertThat(detail.ages()).containsExactly(30, 29);
    }

    @Test
    void countResponseSearchDetail_agesUnmodifiable() {
        var detail = new CountResponse.SearchDetail("hotel-1", CHECK_IN, CHECK_OUT, List.of(30, 29));

        assertThatThrownBy(() -> detail.ages().add(99))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void searchRequest_nullElementsPreservedForBeanValidation() {
        var ages = new ArrayList<Integer>();
        ages.add(30);
        ages.add(null);
        var request = new SearchRequest("hotel-1", CHECK_IN, CHECK_OUT, ages);

        assertThat(request.ages()).containsExactly(30, null);
    }

    @Test
    void searchRequest_nullAgesNotCopied() {
        var request = new SearchRequest("hotel-1", CHECK_IN, CHECK_OUT, null);

        assertThat(request.ages()).isNull();
    }

    @Test
    void countResponseSearchDetail_nullAgesNotCopied() {
        var detail = new CountResponse.SearchDetail("hotel-1", CHECK_IN, CHECK_OUT, null);

        assertThat(detail.ages()).isNull();
    }
    @Test
    void errorResponseShouldCopyErrorsAndExposeAnUnmodifiableList() {
        var errors = new ArrayList<>(List.of("Invalid hotelId"));
        var response = new ErrorResponse(400, "Validation failed", errors);
        errors.clear();
        assertThat(response.errors()).containsExactly("Invalid hotelId");
        assertThatThrownBy(() -> response.errors().add("other"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
