package com.sling.hotel.application.usecase;

import com.sling.hotel.application.ports.in.CountSearchPort;
import com.sling.hotel.application.ports.out.SearchRepositoryPort;
import com.sling.hotel.domain.exception.SearchNotFoundException;
import com.sling.hotel.domain.model.HotelSearch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CountSearchUseCaseTest {

    @Mock
    private SearchRepositoryPort searchRepository;

    @InjectMocks
    private CountSearchUseCase useCase;

    private static final HotelSearch SEARCH = new HotelSearch(
            "search-1", "hotel-1",
            LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
            List.of(30, 29, 1, 3)
    );

    @Test
    void shouldReturnSearchAndCount() {
        when(searchRepository.findWithCount("search-1"))
                .thenReturn(Optional.of(new SearchRepositoryPort.SearchCount(SEARCH, 5L)));

        CountSearchPort.Result result = useCase.count("search-1");

        assertAll(
                () -> assertThat(result.search()).isEqualTo(SEARCH),
                () -> assertThat(result.count()).isEqualTo(5L)
        );
    }

    @Test
    void shouldThrowWhenSearchNotFound() {
        when(searchRepository.findWithCount("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> useCase.count("missing"))
                .isInstanceOf(SearchNotFoundException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void shouldPropagateRepositoryError() {
        when(searchRepository.findWithCount("search-1")).thenThrow(new RuntimeException("DB error"));

        assertThatThrownBy(() -> useCase.count("search-1"))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("DB error");
    }
}
