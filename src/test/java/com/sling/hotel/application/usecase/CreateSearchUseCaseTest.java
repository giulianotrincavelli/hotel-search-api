package com.sling.hotel.application.usecase;

import com.sling.hotel.application.model.CreateSearchInput;
import com.sling.hotel.application.ports.out.SearchPublisherPort;
import com.sling.hotel.domain.model.HotelSearch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class CreateSearchUseCaseTest {

    @Mock
    private SearchPublisherPort searchPublisher;

    @InjectMocks
    private CreateSearchUseCase useCase;

    private static final CreateSearchInput COMMAND = new CreateSearchInput(
            "hotel-1",
            LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
            List.of(30, 29, 1, 3)
    );

    @Test
    void shouldGenerateSearchIdAndPublish() {
        HotelSearch result = useCase.create(COMMAND);

        assertThat(result.searchId()).isNotBlank();
        assertThat(result.hotelId()).isEqualTo("hotel-1");
        assertThat(result.checkIn()).isEqualTo(LocalDate.of(2023, 12, 29));
        assertThat(result.checkOut()).isEqualTo(LocalDate.of(2023, 12, 31));
        assertThat(result.ages()).containsExactly(30, 29, 1, 3);

        var captor = ArgumentCaptor.forClass(HotelSearch.class);
        verify(searchPublisher).publish(captor.capture());
        assertThat(captor.getValue().searchId()).isEqualTo(result.searchId());
    }

    @Test
    void shouldGenerateUniqueSearchIds() {
        HotelSearch first = useCase.create(COMMAND);
        HotelSearch second = useCase.create(COMMAND);

        assertThat(first.searchId()).isNotEqualTo(second.searchId());
    }

    @Test
    void shouldPropagatePublishFailure() {
        doThrow(new RuntimeException("Kafka down")).when(searchPublisher).publish(any());

        assertThatThrownBy(() -> useCase.create(COMMAND))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Kafka down");
    }
}
