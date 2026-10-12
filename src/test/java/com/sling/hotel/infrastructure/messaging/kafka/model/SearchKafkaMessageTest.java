package com.sling.hotel.infrastructure.messaging.kafka.model;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SearchKafkaMessageTest {

    @Test
    void agesShouldBeImmutableViaCopy() {
        List<Integer> original = new ArrayList<>(List.of(30, 29, 1, 3));
        var message = new SearchKafkaMessage("s1", "h1", "2023-12-29", "2023-12-31", original);

        original.add(99);

        assertThat(message.ages()).containsExactly(30, 29, 1, 3);
    }

    @Test
    void agesShouldNotBeModifiableAfterCreation() {
        var message = new SearchKafkaMessage("s1", "h1", "2023-12-29", "2023-12-31", List.of(30, 29));

        assertThatThrownBy(() -> message.ages().add(99))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldPreserveAllFields() {
        var message = new SearchKafkaMessage("s1", "h1", "2023-12-29", "2023-12-31", List.of(30, 29));

        assertThat(message.searchId()).isEqualTo("s1");
        assertThat(message.hotelId()).isEqualTo("h1");
        assertThat(message.checkIn()).isEqualTo("2023-12-29");
        assertThat(message.checkOut()).isEqualTo("2023-12-31");
        assertThat(message.ages()).containsExactly(30, 29);
    }

    @Test
    void shouldAllowNullAges() {
        var message = new SearchKafkaMessage("s1", "h1", "2023-12-29", "2023-12-31", null);
        assertThat(message.ages()).isNull();
    }
}
