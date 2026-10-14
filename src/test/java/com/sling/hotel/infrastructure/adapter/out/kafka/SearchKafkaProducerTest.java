package com.sling.hotel.infrastructure.adapter.out.kafka;

import com.sling.hotel.application.exception.PublishException;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.logging.CorrelationId;
import com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage;
import com.sling.hotel.support.LogCapture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SearchKafkaProducerTest {

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    private static final String TOPIC = "hotel_availability_searches";
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static final HotelSearch SEARCH = new HotelSearch(
            "search-1", "hotel-1",
            LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
            List.of(30, 29, 1, 3)
    );

    @SuppressWarnings("unchecked")
    @Test
    void shouldPublishToCorrectTopicWithSearchIdAsKey() {
        var producer = new SearchKafkaProducer(kafkaTemplate, TOPIC, SEND_TIMEOUT);

        var metadata = new RecordMetadata(
                new TopicPartition(TOPIC, 0), 0, 0, 0, 0, 0);
        var sendResult = new SendResult<String, Object>(null, metadata);
        var future = CompletableFuture.completedFuture(sendResult);

        ArgumentCaptor<ProducerRecord<String, Object>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        when(kafkaTemplate.send(recordCaptor.capture())).thenReturn(future);
        MDC.put(CorrelationId.MDC_KEY, "client-correlation");

        try (var logs = new LogCapture(SearchKafkaProducer.class.getName())) {
            producer.publish(SEARCH);
            assertThat(logs.events()).anySatisfy(event -> {
                assertAll(
                        () -> assertThat(event.getFormattedMessage()).contains("searchId=search-1", "correlationId=client-correlation", "partition=0", "offset=0"),
                        () -> assertThat(event.getMDCPropertyMap()).containsEntry("correlationId", "client-correlation"));
            });
        }

        var producerRecord = recordCaptor.getValue();
        assertAll(
                () -> assertThat(producerRecord.topic()).isEqualTo(TOPIC),
                () -> assertThat(producerRecord.key()).isEqualTo("search-1"),
                () -> assertThat(new String(producerRecord.headers().lastHeader(CorrelationId.HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("client-correlation"),
                () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isEqualTo("client-correlation"));
        SearchKafkaMessage message = (SearchKafkaMessage) producerRecord.value();
        assertAll(
                () -> assertThat(message.searchId()).isEqualTo("search-1"),
                () -> assertThat(message.hotelId()).isEqualTo("hotel-1"),
                () -> assertThat(message.checkIn()).isEqualTo("2023-12-29"),
                () -> assertThat(message.checkOut()).isEqualTo("2023-12-31"),
                () -> assertThat(message.ages()).containsExactly(30, 29, 1, 3)
        );
    }

    @Test
    void shouldThrowPublishExceptionOnBrokerFailure() {
        var producer = new SearchKafkaProducer(kafkaTemplate, TOPIC, SEND_TIMEOUT);

        var future = new CompletableFuture<SendResult<String, Object>>();
        future.completeExceptionally(new TimeoutException("Broker unavailable"));

        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenReturn(future);

        assertAll(
                () -> assertThatThrownBy(() -> producer.publish(SEARCH))
                .isInstanceOf(PublishException.class)
                .hasCauseInstanceOf(TimeoutException.class)
                .hasMessageContaining("Failed to publish")
                .hasMessageContaining("searchId=search-1")
                .hasMessageContaining("topic=" + TOPIC)
                        .hasMessageContaining("correlationId="),
                () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
    }

    @Test
    void synchronousKafkaTimeoutShouldBecomePublishException() {
        var failure = new TimeoutException("No broker metadata");
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenThrow(failure);
        var producer = new SearchKafkaProducer(kafkaTemplate, TOPIC, SEND_TIMEOUT);
        assertAll(
                () -> assertThatThrownBy(() -> producer.publish(SEARCH))
                .isInstanceOf(PublishException.class).hasCause(failure),
                () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
    }

    @Test
    void asynchronousKafkaSendExceptionShouldBecomePublishException() {
        var producerRecord = new ProducerRecord<String, Object>(TOPIC, SEARCH.searchId(), SEARCH);
        var failure = new KafkaProducerException(producerRecord, "Send failed",
                new NetworkException("Connection lost"));
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenReturn(CompletableFuture.failedFuture(failure));
        var producer = new SearchKafkaProducer(kafkaTemplate, TOPIC, SEND_TIMEOUT);
        assertThatThrownBy(() -> producer.publish(SEARCH))
                .isInstanceOf(PublishException.class).hasCause(failure);
    }

    @Test
    void synchronousProgrammingErrorShouldNotBecomePublishException() {
        var failure = new IllegalStateException("Invalid producer state");
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenThrow(failure);
        var producer = new SearchKafkaProducer(kafkaTemplate, TOPIC, SEND_TIMEOUT);
        assertThatThrownBy(() -> producer.publish(SEARCH))
                .isSameAs(failure);
    }

    @Test
    void asynchronousProgrammingErrorShouldNotBecomePublishException() {
        var failure = new IllegalArgumentException("Invalid record");
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenReturn(CompletableFuture.failedFuture(failure));
        var producer = new SearchKafkaProducer(kafkaTemplate, TOPIC, SEND_TIMEOUT);
        assertThatThrownBy(() -> producer.publish(SEARCH))
                .isSameAs(failure);
    }

    @Test
    void serializationErrorShouldNotBecomeInfrastructureError() {
        var failure = new SerializationException("Invalid payload");
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenReturn(CompletableFuture.failedFuture(failure));
        var producer = new SearchKafkaProducer(kafkaTemplate, TOPIC, SEND_TIMEOUT);
        assertThatThrownBy(() -> producer.publish(SEARCH))
                .isSameAs(failure);
    }

    @Test
    void acknowledgmentTimeoutShouldBecomePublishException() {
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenReturn(new CompletableFuture<>());
        var producer = new SearchKafkaProducer(kafkaTemplate, TOPIC, Duration.ofMillis(1));
        assertAll(
                () -> assertThatThrownBy(() -> producer.publish(SEARCH))
                .isInstanceOf(PublishException.class).hasCauseInstanceOf(java.util.concurrent.TimeoutException.class),
                () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
    }

    @Test
    void interruptedPublicationShouldRestoreInterruptAndMdc() {
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenReturn(new CompletableFuture<>());
        MDC.put(CorrelationId.MDC_KEY, "interrupted-request");
        Thread.currentThread().interrupt();
        try {
            var producer = new SearchKafkaProducer(kafkaTemplate, TOPIC, SEND_TIMEOUT);
        assertThatThrownBy(() -> producer.publish(SEARCH))
                    .isInstanceOf(PublishException.class).hasCauseInstanceOf(InterruptedException.class);
            assertAll(
                    () -> assertThat(Thread.currentThread().isInterrupted()).isTrue(),
                    () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isEqualTo("interrupted-request"));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void invalidKafkaConfigurationShouldNotBecomeInfrastructureError() {
        var failure = new ConfigException("Invalid bootstrap servers");
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, Object>>any())).thenThrow(failure);
        var producer = new SearchKafkaProducer(kafkaTemplate, TOPIC, SEND_TIMEOUT);
        assertThatThrownBy(() -> producer.publish(SEARCH))
                .isSameAs(failure);
    }

    @Test
    void wrappedConfigurationErrorShouldNotBecomeInfrastructureError() {
        var invalidConfiguration = new ConfigException("Invalid bootstrap servers");
        var failure = new org.apache.kafka.common.KafkaException("Failed to construct producer", invalidConfiguration);
        when(kafkaTemplate.send(ArgumentMatchers.<ProducerRecord<String, Object>>any())).thenThrow(failure);
        var producer = new SearchKafkaProducer(kafkaTemplate, TOPIC, SEND_TIMEOUT);
        assertThatThrownBy(() -> producer.publish(SEARCH))
                .isSameAs(invalidConfiguration);
    }
}
