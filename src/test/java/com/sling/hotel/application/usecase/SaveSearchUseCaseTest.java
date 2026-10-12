package com.sling.hotel.application.usecase;

import com.sling.hotel.application.ports.out.SearchRepositoryPort;
import com.sling.hotel.domain.model.HotelSearch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SaveSearchUseCaseTest {

    @Mock
    private SearchRepositoryPort searchRepository;

    @InjectMocks
    private SaveSearchUseCase useCase;

    private static final HotelSearch SEARCH = new HotelSearch(
            "search-1", "hotel-1",
            LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
            List.of(30, 29, 1, 3)
    );

    @Test
    void shouldPersistSearch() {
        useCase.save(SEARCH);

        verify(searchRepository).save(SEARCH);
    }

    @Test
    void shouldPropagatePersistenceFailure() {
        doThrow(new RuntimeException("DB error")).when(searchRepository).save(SEARCH);

        assertThatThrownBy(() -> useCase.save(SEARCH))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("DB error");
    }
}
