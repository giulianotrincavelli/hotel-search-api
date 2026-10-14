package com.sling.hotel.infrastructure.adapter.in.kafka;

import com.sling.hotel.application.ports.in.SaveSearchPort;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.logging.CorrelationId;
import com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import java.util.stream.Stream;
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

    private static ConsumerRecord<String, SearchKafkaMessage> consumerRecord(SearchKafkaMessage message) {
        return new ConsumerRecord<>(TOPIC, 0, 42L, message.searchId(), message);
    }

    @Test
    void shouldLoadHeaderAndClearMdcOnVirtualThreadIncludingFailures() throws Exception {
        var message = new SearchKafkaMessage("search-context", "hotel-1", "2023-12-29", "2023-12-31", List.of(30));
        var consumerRecord = consumerRecord(message);
        consumerRecord.headers().add(CorrelationId.HEADER, "message-correlation".getBytes(StandardCharsets.UTF_8));
        doAnswer(inv -> {
            assertAll(
                    () -> assertThat(Thread.currentThread().isVirtual()).isTrue(),
                    () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isEqualTo("message-correlation"));
            throw new RuntimeException("DB unavailable");
        }).when(saveSearchPort).save(ArgumentMatchers.any());

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(() -> {
                MDC.put(CorrelationId.MDC_KEY, "stale-context");
                assertAll(
                        () -> assertThatThrownBy(() -> consumer.consume(consumerRecord)).hasMessage("DB unavailable"),
                        () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
            }).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void shouldGenerateIndependentCorrelationIdForMessageWithoutHeaderAndClearMdc() {
        var consumerRecord = consumerRecord(new SearchKafkaMessage("search-no-header", "hotel-1", "2023-12-29", "2023-12-31", List.of(30)));
        MDC.put(CorrelationId.MDC_KEY, "previous-message");
        doAnswer(inv -> {
            String id = MDC.get(CorrelationId.MDC_KEY);
            assertAll(
                    () -> assertThat(UUID.fromString(id)).isNotNull(),
                    () -> assertThat(id).isNotEqualTo("previous-message").isNotEqualTo(consumerRecord.key()),
                    () -> assertThat(new String(consumerRecord.headers().lastHeader(CorrelationId.HEADER).value(), StandardCharsets.UTF_8)).isEqualTo(id));
            return null;
        }).when(saveSearchPort).save(ArgumentMatchers.any());
        consumer.consume(consumerRecord);
        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }

    @Test
    void shouldConvertMessageAndInvokeSave() {
        var message = new SearchKafkaMessage(
                "search-1", "hotel-1", "2023-12-29", "2023-12-31",
                List.of(30, 29, 1, 3)
        );

        consumer.consume(consumerRecord(message));

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

        var failure = new IllegalStateException("DB error");
        doThrow(failure).when(saveSearchPort).save(ArgumentMatchers.any());

        var incoming = consumerRecord(message);
        assertAll(
                () -> assertThatThrownBy(() -> consumer.consume(incoming)).isSameAs(failure),
                () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidMessages")
    void shouldRejectInvalidKafkaMessage(String scenario, SearchKafkaMessage message) {
        var incoming = consumerRecord(message);
        assertAll(
                () -> assertThatThrownBy(() -> consumer.consume(incoming))
                .isInstanceOf(InvalidKafkaMessageException.class)
                        .hasMessageContaining("Invalid Kafka message"),
                () -> Mockito.verifyNoInteractions(saveSearchPort),
                () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
    }

    static Stream<Arguments> invalidMessages() {
        return Stream.of(
                Arguments.of("invalid date",
                        new SearchKafkaMessage("search-1", "hotel-1", "not-a-date", "2023-12-31", List.of(30))),
                Arguments.of("null searchId",
                        new SearchKafkaMessage(null, "hotel-1", "2023-12-29", "2023-12-31", List.of(30))),
                Arguments.of("null checkIn",
                        new SearchKafkaMessage("search-1", "hotel-1", null, "2023-12-31", List.of(30))));
    }

    @Test
    void shouldPreserveAgesOrderThroughConversion() {
        var message = new SearchKafkaMessage(
                "search-1", "hotel-1", "2023-12-29", "2023-12-31",
                List.of(3, 29, 30, 1)
        );

        consumer.consume(consumerRecord(message));

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

        consumer.consume(consumerRecord(message));

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

        var incoming = consumerRecord(message);
        assertThatThrownBy(() -> consumer.consume(incoming))
                .isInstanceOf(InvalidKafkaMessageException.class)
                .hasMessageContaining("Invalid Kafka message");
    }

    @Test
    void shouldThrowInvalidKafkaMessageExceptionOnTooManyAges() {
        var message = new SearchKafkaMessage(
                "search-1", "hotel-1", "2023-12-29", "2023-12-31",
                java.util.Collections.nCopies(51, 50)
        );

        var incoming = consumerRecord(message);
        assertThatThrownBy(() -> consumer.consume(incoming))
                .isInstanceOf(InvalidKafkaMessageException.class)
                .hasMessageContaining("Invalid Kafka message");
    }

    @Test
    void shouldAcceptHotelIdOf50Characters() {
        var message = new SearchKafkaMessage(
                "search-1", "a".repeat(50), "2023-12-29", "2023-12-31",
                List.of(30)
        );

        consumer.consume(consumerRecord(message));

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

        consumer.consume(consumerRecord(message));

        ArgumentCaptor<HotelSearch> captor = ArgumentCaptor.forClass(HotelSearch.class);
        verify(saveSearchPort).save(captor.capture());
        assertThat(captor.getValue().ages()).hasSize(50);
    }

    @Test
    void nullValueShouldBeRejectedAsPermanentInvalidMessageAndClearMdc() {
        var consumerRecord = new ConsumerRecord<String, SearchKafkaMessage>("hotel_availability_searches", 0, 5, "null-search", null);
        assertAll(
                () -> assertThatThrownBy(() -> consumer.consume(consumerRecord))
                .isInstanceOf(InvalidKafkaMessageException.class)
                        .hasMessage("Kafka message value must not be null"),
                () -> Mockito.verifyNoInteractions(saveSearchPort),
                () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
    }

    @Test
    void disabledDebugLoggingShouldNotProduceDetailsOrReadOffset() {
        var message = new SearchKafkaMessage("search-1", "hotel-1", "2023-12-29", "2023-12-31", List.of(30));
        var incoming = Mockito.spy(consumerRecord(message));
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(SearchKafkaConsumer.class);
        try (var logs = new com.sling.hotel.support.LogCapture(SearchKafkaConsumer.class.getName())) {
            logger.setLevel(ch.qos.logback.classic.Level.INFO);
            consumer.consume(incoming);
            assertAll(
                    () -> verify(saveSearchPort).save(ArgumentMatchers.any()),
                    () -> Mockito.verify(incoming, Mockito.never()).offset(),
                    () -> assertThat(logs.events()).isEmpty(),
                    () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
        }
    }

}
