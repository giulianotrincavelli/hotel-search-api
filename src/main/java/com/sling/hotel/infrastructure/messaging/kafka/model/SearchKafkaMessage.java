package com.sling.hotel.infrastructure.messaging.kafka.model;

import java.util.List;

// Dates serialized as ISO-8601 strings (yyyy-MM-dd).
public record SearchKafkaMessage(
        String searchId,
        String hotelId,
        String checkIn,
        String checkOut,
        List<Integer> ages
) {

    public SearchKafkaMessage {
        ages = ages == null ? null : List.copyOf(ages);
    }
}
