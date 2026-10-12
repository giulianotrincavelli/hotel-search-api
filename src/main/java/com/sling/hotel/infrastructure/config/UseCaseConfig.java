package com.sling.hotel.infrastructure.config;

import com.sling.hotel.application.ports.in.CountSearchPort;
import com.sling.hotel.application.ports.in.CreateSearchPort;
import com.sling.hotel.application.ports.in.SaveSearchPort;
import com.sling.hotel.application.ports.out.SearchPublisherPort;
import com.sling.hotel.application.ports.out.SearchRepositoryPort;
import com.sling.hotel.application.usecase.CountSearchUseCase;
import com.sling.hotel.application.usecase.CreateSearchUseCase;
import com.sling.hotel.application.usecase.SaveSearchUseCase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class UseCaseConfig {

    @Bean
    CreateSearchPort createSearchPort(SearchPublisherPort searchPublisher) {
        return new CreateSearchUseCase(searchPublisher);
    }

    @Bean
    CountSearchPort countSearchPort(SearchRepositoryPort searchRepository) {
        return new CountSearchUseCase(searchRepository);
    }

    @Bean
    SaveSearchPort saveSearchPort(SearchRepositoryPort searchRepository) {
        return new SaveSearchUseCase(searchRepository);
    }
}
