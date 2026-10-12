package com.sling.hotel.infrastructure.config;

import com.sling.hotel.domain.exception.IdempotencyConflictException;
import com.sling.hotel.infrastructure.adapter.in.kafka.InvalidKafkaMessageException;
import com.sling.hotel.infrastructure.adapter.out.kafka.SearchKafkaProducer;
import com.sling.hotel.infrastructure.logging.CorrelationId;
import com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage;
import com.sling.hotel.support.LogCapture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.SerializationUtils;
import tools.jackson.databind.exc.MismatchedInputException;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaConfigTest {

    @Mock
    private KafkaOperations<String, Object> kafkaOperations;

    private static final HotelSearchKafkaProperties DEFAULT_PROPS = new HotelSearchKafkaProperties(
            "hotel_availability_searches",
            ".DLT",
            new HotelSearchKafkaProperties.Retry(Duration.ofSeconds(5), 60),
            new HotelSearchKafkaProperties.Producer(Duration.ofSeconds(10)),
            new HotelSearchKafkaProperties.Dlt(Duration.ofSeconds(15))
    );

    private final KafkaConfig kafkaConfig = new KafkaConfig(DEFAULT_PROPS);

    @Test
    void invalidKafkaMessageExceptionShouldBeClassifiedAsNotRetryable() {
        DefaultErrorHandler handler = kafkaConfig.kafkaErrorHandler(kafkaOperations);
        var removed = handler.removeClassification(InvalidKafkaMessageException.class);
        assertThat(removed).isNotNull().isFalse();
    }

    @Test
    void idempotencyConflictExceptionShouldBeClassifiedAsNotRetryable() {
        DefaultErrorHandler handler = kafkaConfig.kafkaErrorHandler(kafkaOperations);
        var removed = handler.removeClassification(IdempotencyConflictException.class);
        assertThat(removed).isNotNull().isFalse();
    }

    @Test
    void dltTopicNameShouldCombineTopicAndSuffix() {
        assertThat(DEFAULT_PROPS.dltTopic())
                .isEqualTo("hotel_availability_searches.DLT");
    }

    @Test
    void dltTopicNameShouldRespectCustomSuffix() {
        var props = new HotelSearchKafkaProperties(
                "my-topic", ".dead",
                DEFAULT_PROPS.retry(), DEFAULT_PROPS.producer(), DEFAULT_PROPS.dlt()
        );
        assertThat(props.dltTopic()).isEqualTo("my-topic.dead");
    }

    @Test
    void searchesTopicBeanShouldUseConfiguredTopicName() {
        assertThat(kafkaConfig.searchesTopic().name()).isEqualTo("hotel_availability_searches");
    }

    @Test
    void dltTopicBeanShouldUseComputedDltName() {
        assertThat(kafkaConfig.dltTopic().name()).isEqualTo("hotel_availability_searches.DLT");
    }

    @Test
    void propertiesShouldExposeRetryConfiguration() {
        assertThat(DEFAULT_PROPS.retry().interval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(DEFAULT_PROPS.retry().maxAttempts()).isEqualTo(60);
    }

    @Test
    void propertiesShouldExposeSeparateTimeouts() {
        assertThat(DEFAULT_PROPS.producer().sendTimeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(DEFAULT_PROPS.dlt().sendTimeout()).isEqualTo(Duration.ofSeconds(15));
    }

    @Test
    void transientFailuresShouldRetryTwiceWithConfiguredIntervalBeforeRecovery() {
        var props = new HotelSearchKafkaProperties("retry-topic", ".DLT",
                new HotelSearchKafkaProperties.Retry(Duration.ofMillis(100), 2),
                DEFAULT_PROPS.producer(), DEFAULT_PROPS.dlt());
        when(kafkaOperations.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenAnswer(inv -> {
                    ProducerRecord<String, Object> sent = inv.getArgument(0);
                    return CompletableFuture.completedFuture(new SendResult<>(sent,
                            new RecordMetadata(new TopicPartition(sent.topic(), 0), 0, 0, 0, 0, 0)));
                });
        var handler = new KafkaConfig(props).kafkaErrorHandler(kafkaOperations);
        var record = new ConsumerRecord<>("retry-topic", 0, 1L, "retry-id",
                new SearchKafkaMessage("retry-id", "hotel", "2026-12-29", "2026-12-31", List.of(30)));
        var consumer = mock(Consumer.class);
        var container = mock(MessageListenerContainer.class);
        when(container.isRunning()).thenReturn(true);
        var failure = new IllegalStateException("Database unavailable");
        long before = System.nanoTime();
        assertThat(handler.handleOne(failure, record, consumer, container)).isFalse();
        long afterFirst = System.nanoTime();
        assertThat(handler.handleOne(failure, record, consumer, container)).isFalse();
        long afterSecond = System.nanoTime();
        Mockito.verify(kafkaOperations, Mockito.never())
                .send(ArgumentMatchers.<ProducerRecord<String, Object>>any());
        assertThat(handler.handleOne(failure, record, consumer, container)).isTrue();
        assertThat(Duration.ofNanos(afterFirst - before)).isGreaterThanOrEqualTo(Duration.ofMillis(100));
        assertThat(Duration.ofNanos(afterSecond - afterFirst)).isGreaterThanOrEqualTo(Duration.ofMillis(100));
        Mockito.verify(kafkaOperations).send(ArgumentMatchers.<ProducerRecord<String, Object>>any());
        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }

    @SuppressWarnings("unchecked")
    @Test
    void dltShouldRetainHeaderAndUseCorrelationMdcDuringSendAndRecoveryLog() {
        var props = new HotelSearchKafkaProperties("test-topic", ".DLT",
                new HotelSearchKafkaProperties.Retry(Duration.ofMillis(1), 0),
                DEFAULT_PROPS.producer(), new HotelSearchKafkaProperties.Dlt(Duration.ofSeconds(2)));
        when(kafkaOperations.send(any(ProducerRecord.class))).thenAnswer(inv -> {
            ProducerRecord<String, Object> sent = inv.getArgument(0);
            assertThat(sent.topic()).isEqualTo("test-topic.DLT");
            assertThat(new String(sent.headers().lastHeader(CorrelationId.HEADER).value(), StandardCharsets.UTF_8))
                    .isEqualTo("dlt-correlation");
            assertThat(MDC.get(CorrelationId.MDC_KEY)).isEqualTo("dlt-correlation");
            return CompletableFuture.completedFuture(new SendResult<>(sent,
                    new RecordMetadata(new TopicPartition(sent.topic(), 0), 0, 0, 0, 0, 0)));
        });
        var message = new SearchKafkaMessage("dlt-search", "hotel-1", "invalid", "2023-12-31", List.of(30));
        var record = new ConsumerRecord<>("test-topic", 0, 42L, "dlt-search", message);
        record.headers().add(CorrelationId.HEADER, "dlt-correlation".getBytes(StandardCharsets.UTF_8));
        var handler = new KafkaConfig(props).kafkaErrorHandler(kafkaOperations);
        try (var logs = new LogCapture(KafkaConfig.class.getName())) {
            handler.handleRemaining(new InvalidKafkaMessageException("invalid", new IllegalArgumentException()),
                    List.of(record), mock(Consumer.class), mock(MessageListenerContainer.class));
            assertThat(logs.events()).anySatisfy(event -> {
                assertThat(event.getFormattedMessage()).contains("recovered to DLT", "correlationId=dlt-correlation", "offset=42");
                assertThat(event.getMDCPropertyMap()).containsEntry("correlationId", "dlt-correlation");
            });
        }
        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }

    @SuppressWarnings("unchecked")
    @Test
    void dltSendFailureShouldNotBeSilentlySwallowed() {
        var noRetryProps = new HotelSearchKafkaProperties(
                "test-topic", ".DLT",
                new HotelSearchKafkaProperties.Retry(Duration.ofMillis(1), 0),
                DEFAULT_PROPS.producer(),
                new HotelSearchKafkaProperties.Dlt(Duration.ofSeconds(2))
        );
        var noRetryConfig = new KafkaConfig(noRetryProps);

        var failedFuture = new CompletableFuture<SendResult<String, Object>>();
        failedFuture.completeExceptionally(new TimeoutException("DLT broker unavailable"));
        when(kafkaOperations.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        DefaultErrorHandler handler = noRetryConfig.kafkaErrorHandler(kafkaOperations);
        var record = new ConsumerRecord<>("test-topic", 0, 0L, "key",
                new SearchKafkaMessage("key", "hotel", "2026-12-29", "2026-12-31", List.of(30)));

        var consumer = mock(Consumer.class);
        try (var logs = new LogCapture(KafkaConfig.class.getName())) {
            assertThatThrownBy(() -> handler.handleRemaining(
                    new RuntimeException("original"), List.of(record), consumer, mock(MessageListenerContainer.class)))
                    .hasMessage("Record in retry and not yet recovered");
            assertThat(logs.events()).anySatisfy(event -> {
                assertThat(event.getFormattedMessage()).contains("DLT recovery failed", "searchId=key", "dltTopic=test-topic.DLT");
                assertThat(event.getThrowableProxy()).isNotNull();
            });
        }
        Mockito.verify(kafkaOperations).send(ArgumentMatchers.<ProducerRecord<String, Object>>any());
        Mockito.verify(consumer).seek(new TopicPartition("test-topic", 0), 0L);
        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }
    private static JsonMapper applicationMapper() {
        var builder = JsonMapper.builder();
        var config = new JacksonConfig();
        config.strictIntegerTypesCustomizer().customize(builder);
        config.strictStringTypesCustomizer().customize(builder);
        config.strictLocalDateCustomizer().customize(builder);
        config.strictLocalDateNoTimestampsCustomizer().customize(builder);
        return builder.build();
    }

    @ParameterizedTest
    @ValueSource(strings = {"[\"30\"]", "[30.9]", "[true]", "[null]", "{}"})
    void configuredDeserializerShouldRejectInvalidAgeTypes(String ages) {
        var factory = kafkaConfig.kafkaConsumerFactory(new KafkaProperties(), applicationMapper());
        var deserializer = factory.getValueDeserializer();
        deserializer.configure(factory.getConfigurationProperties(), false);
        byte[] json = ("{\"searchId\":\"strict-id\",\"hotelId\":\"hotel\",\"checkIn\":\"2026-12-29\",\"checkOut\":\"2026-12-31\",\"ages\":" + ages + "}")
                .getBytes(StandardCharsets.UTF_8);
        var headers = new RecordHeaders();
        assertThat(deserializer.deserialize("test-topic", headers, json)).isNull();
        var header = headers.lastHeader(KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
        assertThat(header).isNotNull();
        var failure = SerializationUtils.byteArrayToDeserializationException(
                new LogAccessor(KafkaConfigTest.class), header);
        assertThat(failure.getData()).containsExactly(json);
    }

    @Test
    void configuredSerdesShouldRoundTripValidMessageAndPreserveOrder() {
        var mapper = applicationMapper();
        var kafkaProperties = new KafkaProperties();
        var producerFactory = kafkaConfig.kafkaProducerFactory(kafkaProperties, mapper);
        var consumerFactory = kafkaConfig.kafkaConsumerFactory(kafkaProperties, mapper);
        var serializer = producerFactory.getValueSerializer();
        var deserializer = consumerFactory.getValueDeserializer();
        serializer.configure(producerFactory.getConfigurationProperties(), false);
        deserializer.configure(consumerFactory.getConfigurationProperties(), false);
        var message = new SearchKafkaMessage("round-trip", "hotel", "2026-12-29", "2026-12-31", List.of(30, 1, 30));
        var headers = new RecordHeaders();
        byte[] json = serializer.serialize("test-topic", headers, message);
        assertThat(deserializer.deserialize("test-topic", headers, json)).isEqualTo(message);
        assertThat(serializer.serialize("test-topic", headers, null)).isNull();
        assertThat(deserializer.deserialize("test-topic", headers, (byte[]) null)).isNull();
    }

    @Test
    void deserializationFailureShouldRecoverOriginalBytesThroughConfiguredSerializer() {
        byte[] original = "{not valid json!!!}".getBytes(StandardCharsets.UTF_8);
        var kafkaProperties = new KafkaProperties();
        var mapper = applicationMapper();
        var consumerFactory = kafkaConfig.kafkaConsumerFactory(kafkaProperties, mapper);
        var deserializer = consumerFactory.getValueDeserializer();
        deserializer.configure(consumerFactory.getConfigurationProperties(), false);
        var record = new ConsumerRecord<String, SearchKafkaMessage>("test-topic", 0, 7L, "bad-key", null);
        record.headers().add(CorrelationId.HEADER, "raw-dlt-correlation".getBytes(StandardCharsets.UTF_8));
        assertThat(deserializer.deserialize(record.topic(), record.headers(), original)).isNull();
        var failure = SerializationUtils.byteArrayToDeserializationException(
                new LogAccessor(KafkaConfigTest.class),
                record.headers().lastHeader(KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER));
        var producerFactory = kafkaConfig.kafkaProducerFactory(kafkaProperties, mapper);
        var serializer = producerFactory.getValueSerializer();
        serializer.configure(producerFactory.getConfigurationProperties(), false);
        when(kafkaOperations.send(ArgumentMatchers.<ProducerRecord<String, Object>>any())).thenAnswer(inv -> {
            ProducerRecord<String, Object> sent = inv.getArgument(0);
            assertThat(sent.topic()).isEqualTo("hotel_availability_searches.DLT");
            assertThat(sent.key()).isEqualTo("bad-key");
            assertThat(sent.value()).isInstanceOf(byte[].class);
            assertThat(serializer.serialize(sent.topic(), sent.headers(), sent.value())).containsExactly(original);
            assertThat(new String(sent.headers().lastHeader(CorrelationId.HEADER).value(), StandardCharsets.UTF_8))
                    .isEqualTo("raw-dlt-correlation");
            return CompletableFuture.completedFuture(new SendResult<>(sent,
                    new RecordMetadata(new TopicPartition(sent.topic(), 0), 0, 0, 0, 0, 0)));
        });
        var handler = kafkaConfig.kafkaErrorHandler(kafkaOperations);
        assertThat(handler.handleOne(failure, record, mock(Consumer.class), mock(MessageListenerContainer.class))).isTrue();
        Mockito.verify(kafkaOperations).send(ArgumentMatchers.<ProducerRecord<String, Object>>any());
        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }

    @Test
    void nullMessageShouldRecoverImmediatelyWithoutRetry() {
        var record = new ConsumerRecord<String, SearchKafkaMessage>("test-topic", 0, 8, "null-key", null);
        when(kafkaOperations.send(ArgumentMatchers.<ProducerRecord<String, Object>>any())).thenAnswer(inv -> {
            ProducerRecord<String, Object> sent = inv.getArgument(0);
            assertThat(sent.value()).isNull();
            return CompletableFuture.completedFuture(new SendResult<>(sent,
                    new RecordMetadata(new TopicPartition(sent.topic(), 0), 0, 0, 0, 0, 0)));
        });
        var handler = kafkaConfig.kafkaErrorHandler(kafkaOperations);
        long start = System.nanoTime();
        assertThat(handler.handleOne(new InvalidKafkaMessageException("null message", null), record,
                mock(Consumer.class), mock(MessageListenerContainer.class))).isTrue();
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(DEFAULT_PROPS.retry().interval());
        Mockito.verify(kafkaOperations).send(ArgumentMatchers.<ProducerRecord<String, Object>>any());
    }

    @Test
    void springBootShouldWireConfiguredFactoriesAndStrictMapper() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        JacksonAutoConfiguration.class,
                        KafkaAutoConfiguration.class))
                .withUserConfiguration(KafkaConfig.class, JacksonConfig.class)
                .withPropertyValues(
                        "spring.kafka.admin.auto-create=false",
                        "spring.kafka.producer.acks=all",
                        "spring.kafka.producer.properties.enable.idempotence=true",
                        "spring.kafka.consumer.group-id=configuration-test",
                        "spring.kafka.listener.ack-mode=record",
                        "spring.jackson.deserialization.fail-on-unknown-properties=true",
                        "hotel-search.kafka.topic=test-topic", "hotel-search.kafka.dlt-suffix=.DLT",
                        "hotel-search.kafka.retry.interval=5s", "hotel-search.kafka.retry.max-attempts=60",
                        "hotel-search.kafka.producer.send-timeout=10s", "hotel-search.kafka.dlt.send-timeout=10s")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(SearchKafkaProducer.class);
                    assertThat(context).hasSingleBean(KafkaTemplate.class);
                    var mapper = context.getBean(JsonMapper.class);
                    assertThatThrownBy(() -> mapper.readValue("\"30\"", Integer.class))
                            .isInstanceOf(MismatchedInputException.class);
                    assertThatThrownBy(() -> mapper.readValue("30.9", Integer.class))
                            .isInstanceOf(MismatchedInputException.class);
                    assertThatThrownBy(() -> mapper.readValue("30", String.class))
                            .isInstanceOf(MismatchedInputException.class);
                    var producer = context.getBean(DefaultKafkaProducerFactory.class);
                    assertThat(producer.getConfigurationProperties()).containsEntry("acks", "all")
                            .containsEntry("enable.idempotence", "true");
                    var listener = context.getBean("kafkaListenerContainerFactory",
                            ConcurrentKafkaListenerContainerFactory.class);
                    assertThat(listener.getContainerProperties().getAckMode())
                            .isEqualTo(ContainerProperties.AckMode.RECORD);
                    var executor = (SimpleAsyncTaskExecutor) listener.getContainerProperties().getListenerTaskExecutor();
                    var virtualThread = new CompletableFuture<Boolean>();
                    executor.execute(() -> virtualThread.complete(Thread.currentThread().isVirtual()));
                    assertThat(virtualThread.get(5, TimeUnit.SECONDS)).isTrue();
                });
    }
}
