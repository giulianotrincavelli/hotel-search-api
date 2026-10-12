package com.sling.hotel.infrastructure.adapter.in.kafka;

import com.sling.hotel.application.ports.in.SaveSearchPort;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.logging.CorrelationId;
import com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SearchKafkaConsumerTest {

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Mock
    private SaveSearchPort saveSearchPort;

    @InjectMocks
    private SearchKafkaConsumer consumer;

    private static final String TOPIC = "test-topic";

    private static ConsumerRecord<String, SearchKafkaMessage> record(SearchKafkaMessage message) {
        return new ConsumerRecord<>(TOPIC, 0, 42L, message.searchId(), message);
    }

    @Test
    void shouldLoadHeaderAndClearMdcOnVirtualThreadIncludingFailures() throws Exception {
        var message = new SearchKafkaMessage("search-context", "hotel-1", "2023-12-29", "2023-12-31", List.of(30));
        var record = record(message);
        record.headers().add(CorrelationId.HEADER, "message-correlation".getBytes(StandardCharsets.UTF_8));
        doAnswer(inv -> {
            assertThat(Thread.currentThread().isVirtual()).isTrue();
            assertThat(MDC.get(CorrelationId.MDC_KEY)).isEqualTo("message-correlation");
            throw new RuntimeException("DB unavailable");
        }).when(saveSearchPort).save(ArgumentMatchers.any());

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(() -> {
                MDC.put(CorrelationId.MDC_KEY, "stale-context");
                assertThatThrownBy(() -> consumer.consume(record)).hasMessage("DB unavailable");
                assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
            }).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldGenerateIndependentCorrelationIdForMessageWithoutHeaderAndClearMdc() {
        var record = record(new SearchKafkaMessage("search-no-header", "hotel-1", "2023-12-29", "2023-12-31", List.of(30)));
        MDC.put(CorrelationId.MDC_KEY, "previous-message");
        doAnswer(inv -> {
            String id = MDC.get(CorrelationId.MDC_KEY);
            assertThat(UUID.fromString(id)).isNotNull();
            assertThat(id).isNotEqualTo("previous-message").isNotEqualTo(record.key());
            assertThat(new String(record.headers().lastHeader(CorrelationId.HEADER).value(), StandardCharsets.UTF_8)).isEqualTo(id);
            return null;
        }).when(saveSearchPort).save(ArgumentMatchers.any());
        consumer.consume(record);
        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }

    @Test
    void shouldConvertMessageAndInvokeSave() {
        var message = new SearchKafkaMessage(
                "search-1", "hotel-1", "2023-12-29", "2023-12-31",
                List.of(30, 29, 1, 3)
        );

        consumer.consume(record(message));

        ArgumentCaptor<HotelSearch> captor = ArgumentCaptor.forClass(HotelSearch.class);
        verify(saveSearchPort).save(captor.capture());

        HotelSearch saved = captor.getValue();
        assertAll(
                () -> assertThat(saved.searchId()).isEqualTo("search-1"),
                () -> assertThat(saved.hotelId()).isEqualTo("hotel-1"),
                () -> assertThat(saved.checkIn()).isEqualTo(LocalDate.of(2023, 12, 29)),
                () -> assertThat(saved.checkOut()).isEqualTo(LocalDate.of(2023, 12, 31)),
                () -> assertThat(saved.ages()).containsExactly(30, 29, 1, 3)
        );
    }

    @Test
    void shouldPropagateExceptionOnPersistenceFailure() {
        var message = new SearchKafkaMessage(
                "search-1", "hotel-1", "2023-12-29", "2023-12-31",
                List.of(30)
        );

        doThrow(new RuntimeException("DB error"))
                .when(saveSearchPort).save(ArgumentMatchers.any());

        assertThatThrownBy(() -> consumer.consume(record(message)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("DB error");
    }

    @Test
    void shouldThrowInvalidKafkaMessageExceptionOnInvalidDate() {
        var message = new SearchKafkaMessage(
                "search-1", "hotel-1", "not-a-date", "2023-12-31",
                List.of(30)
        );

        assertThatThrownBy(() -> consumer.consume(record(message)))
                .isInstanceOf(InvalidKafkaMessageException.class)
                .hasMessageContaining("Invalid Kafka message");
    }

    @Test
    void shouldThrowInvalidKafkaMessageExceptionOnNullSearchId() {
        var message = new SearchKafkaMessage(
                null, "hotel-1", "2023-12-29", "2023-12-31",
                List.of(30)
        );

        assertThatThrownBy(() -> consumer.consume(record(message)))
                .isInstanceOf(InvalidKafkaMessageException.class)
                .hasMessageContaining("Invalid Kafka message");
    }

    @Test
    void shouldPreserveAgesOrderThroughConversion() {
        var message = new SearchKafkaMessage(
                "search-1", "hotel-1", "2023-12-29", "2023-12-31",
                List.of(3, 29, 30, 1)
        );

        consumer.consume(record(message));

        ArgumentCaptor<HotelSearch> captor = ArgumentCaptor.forClass(HotelSearch.class);
        verify(saveSearchPort).save(captor.capture());
        assertThat(captor.getValue().ages()).containsExactly(3, 29, 30, 1);
    }

    @Test
    void shouldPreserveRepeatedAges() {
        var message = new SearchKafkaMessage(
                "search-1", "hotel-1", "2023-12-29", "2023-12-31",
                List.of(5, 5, 5)
        );

        consumer.consume(record(message));

        ArgumentCaptor<HotelSearch> captor = ArgumentCaptor.forClass(HotelSearch.class);
        verify(saveSearchPort).save(captor.capture());
        assertThat(captor.getValue().ages()).containsExactly(5, 5, 5);
    }

    @Test
    void shouldThrowInvalidKafkaMessageExceptionOnHotelIdTooLong() {
        var message = new SearchKafkaMessage(
                "search-1", "a".repeat(51), "2023-12-29", "2023-12-31",
                List.of(30)
        );

        assertThatThrownBy(() -> consumer.consume(record(message)))
                .isInstanceOf(InvalidKafkaMessageException.class)
                .hasMessageContaining("Invalid Kafka message");
    }

    @Test
    void shouldThrowInvalidKafkaMessageExceptionOnTooManyAges() {
        var message = new SearchKafkaMessage(
                "search-1", "hotel-1", "2023-12-29", "2023-12-31",
                java.util.Collections.nCopies(51, 50)
        );

        assertThatThrownBy(() -> consumer.consume(record(message)))
                .isInstanceOf(InvalidKafkaMessageException.class)
                .hasMessageContaining("Invalid Kafka message");
    }

    @Test
    void shouldAcceptHotelIdOf50Characters() {
        var message = new SearchKafkaMessage(
                "search-1", "a".repeat(50), "2023-12-29", "2023-12-31",
                List.of(30)
        );

        consumer.consume(record(message));

        ArgumentCaptor<HotelSearch> captor = ArgumentCaptor.forClass(HotelSearch.class);
        verify(saveSearchPort).save(captor.capture());
        assertThat(captor.getValue().hotelId()).hasSize(50);
    }

    @Test
    void shouldAccept50Ages() {
        var message = new SearchKafkaMessage(
                "search-1", "hotel-1", "2023-12-29", "2023-12-31",
                java.util.Collections.nCopies(50, 100)
        );

        consumer.consume(record(message));

        ArgumentCaptor<HotelSearch> captor = ArgumentCaptor.forClass(HotelSearch.class);
        verify(saveSearchPort).save(captor.capture());
        assertThat(captor.getValue().ages()).hasSize(50);
    }

    @Test
    void shouldThrowInvalidKafkaMessageExceptionOnNullCheckIn() {
        var message = new SearchKafkaMessage(
                "search-1", "hotel-1", null, "2023-12-31",
                List.of(30)
        );

        assertThatThrownBy(() -> consumer.consume(record(message)))
                .isInstanceOf(InvalidKafkaMessageException.class)
                .hasMessageContaining("Invalid Kafka message");
    }
    @Test
    void nullValueShouldBeRejectedAsPermanentInvalidMessageAndClearMdc() {
        var record = new ConsumerRecord<String, SearchKafkaMessage>("hotel_availability_searches", 0, 5, "null-search", null);
        assertThatThrownBy(() -> consumer.consume(record))
                .isInstanceOf(InvalidKafkaMessageException.class)
                .hasMessage("Kafka message value must not be null");
        Mockito.verifyNoInteractions(saveSearchPort);
        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }
}
