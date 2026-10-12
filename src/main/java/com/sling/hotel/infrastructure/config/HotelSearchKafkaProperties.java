package com.sling.hotel.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "hotel-search.kafka")
public record HotelSearchKafkaProperties(
        String topic,
        String dltSuffix,
        Retry retry,
        Producer producer,
        Dlt dlt
) {

    public record Retry(
            Duration interval,
            long maxAttempts
    ) {
    }

    public record Producer(
            Duration sendTimeout
    ) {
    }

    public record Dlt(
            Duration sendTimeout
    ) {
    }

    public String dltTopic() {
        return topic + dltSuffix;
    }
}
