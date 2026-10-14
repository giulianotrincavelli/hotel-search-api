package com.sling.hotel.infrastructure.config;

import org.springframework.boot.validation.autoconfigure.ValidationConfigurationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class ValidationConfig {

    @Bean
    Clock clock() {
        return Clock.systemDefaultZone();
    }

    @Bean
    ValidationConfigurationCustomizer validationClock(Clock clock) {
        return configuration -> configuration.clockProvider(() -> clock);
    }
}
