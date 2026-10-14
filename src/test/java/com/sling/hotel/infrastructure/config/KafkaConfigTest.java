package com.sling.hotel.infrastructure.config;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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

import static org.junit.jupiter.api.Assertions.assertAll;
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

    private final KafkaConfig kafkaConfig = new KafkaConfig(DEFAULT_PROPS, 1);

    @Test
    void invalidKafkaMessageExceptionShouldBeClassifiedAsNotRetryable() {
        DefaultErrorHandler handler = kafkaConfig.kafkaErrorHandler(kafkaOperations);
        var removed = handler.removeClassification(InvalidKafkaMessageException.class);
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
        assertAll(
                () -> assertThat(DEFAULT_PROPS.retry().interval()).isEqualTo(Duration.ofSeconds(5)),
                () -> assertThat(DEFAULT_PROPS.retry().maxAttempts()).isEqualTo(60));
    }

    @Test
    void propertiesShouldExposeSeparateTimeouts() {
        assertAll(
                () -> assertThat(DEFAULT_PROPS.producer().sendTimeout()).isEqualTo(Duration.ofSeconds(10)),
                () -> assertThat(DEFAULT_PROPS.dlt().sendTimeout()).isEqualTo(Duration.ofSeconds(15)));
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
        var handler = new KafkaConfig(props, 1).kafkaErrorHandler(kafkaOperations);
        var consumerRecord = new ConsumerRecord<>("retry-topic", 0, 1L, "retry-id",
                new SearchKafkaMessage("retry-id", "hotel", "2026-12-29", "2026-12-31", List.of(30)));
        var consumer = mock(Consumer.class);
        var container = mock(MessageListenerContainer.class);
        when(container.isRunning()).thenReturn(true);
        var failure = new IllegalStateException("Database unavailable");
        long before = System.currentTimeMillis();
        assertThat(handler.handleOne(failure, consumerRecord, consumer, container)).isFalse();
        long afterFirst = System.currentTimeMillis();
        assertThat(handler.handleOne(failure, consumerRecord, consumer, container)).isFalse();
        long afterSecond = System.currentTimeMillis();
        Mockito.verify(kafkaOperations, Mockito.never())
                .send(ArgumentMatchers.<ProducerRecord<String, Object>>any());
        assertAll(
                () -> assertThat(handler.handleOne(failure, consumerRecord, consumer, container)).isTrue(),
                () -> assertThat(Duration.ofMillis(afterFirst - before)).isGreaterThanOrEqualTo(Duration.ofMillis(100)),
                () -> assertThat(Duration.ofMillis(afterSecond - afterFirst)).isGreaterThanOrEqualTo(Duration.ofMillis(100)));
        assertAll(
                () -> Mockito.verify(kafkaOperations).send(ArgumentMatchers.<ProducerRecord<String, Object>>any()),
                () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
    }

    @SuppressWarnings("unchecked")
    @Test
    void dltShouldRetainHeaderAndUseCorrelationMdcDuringSendAndRecoveryLog() {
        var props = new HotelSearchKafkaProperties("test-topic", ".DLT",
                new HotelSearchKafkaProperties.Retry(Duration.ofMillis(1), 0),
                DEFAULT_PROPS.producer(), new HotelSearchKafkaProperties.Dlt(Duration.ofSeconds(2)));
        when(kafkaOperations.send(any(ProducerRecord.class))).thenAnswer(inv -> {
            ProducerRecord<String, Object> sent = inv.getArgument(0);
            assertAll(
                    () -> assertThat(sent.topic()).isEqualTo("test-topic.DLT"),
                    () -> assertThat(new String(sent.headers().lastHeader(CorrelationId.HEADER).value(), StandardCharsets.UTF_8))
                    .isEqualTo("dlt-correlation"),
                    () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isEqualTo("dlt-correlation"));
            return CompletableFuture.completedFuture(new SendResult<>(sent,
                    new RecordMetadata(new TopicPartition(sent.topic(), 0), 0, 0, 0, 0, 0)));
        });
        var message = new SearchKafkaMessage("dlt-search", "hotel-1", "invalid", "2023-12-31", List.of(30));
        var consumerRecord = new ConsumerRecord<>("test-topic", 0, 42L, "dlt-search", message);
        consumerRecord.headers().add(CorrelationId.HEADER, "dlt-correlation".getBytes(StandardCharsets.UTF_8));
        var handler = new KafkaConfig(props, 1).kafkaErrorHandler(kafkaOperations);
        try (var logs = new LogCapture(KafkaConfig.class.getName())) {
            handler.handleRemaining(new InvalidKafkaMessageException("invalid", new IllegalArgumentException()),
                    List.of(consumerRecord), mock(Consumer.class), mock(MessageListenerContainer.class));
            assertThat(logs.events()).anySatisfy(event -> {
                assertAll(
                        () -> assertThat(event.getFormattedMessage()).contains("recovered to DLT", "correlationId=dlt-correlation", "offset=42"),
                        () -> assertThat(event.getMDCPropertyMap()).containsEntry("correlationId", "dlt-correlation"));
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
        var noRetryConfig = new KafkaConfig(noRetryProps, 1);

        var failedFuture = new CompletableFuture<SendResult<String, Object>>();
        failedFuture.completeExceptionally(new TimeoutException("DLT broker unavailable"));
        when(kafkaOperations.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        DefaultErrorHandler handler = noRetryConfig.kafkaErrorHandler(kafkaOperations);
        var consumerRecord = new ConsumerRecord<>("test-topic", 0, 0L, "key",
                new SearchKafkaMessage("key", "hotel", "2026-12-29", "2026-12-31", List.of(30)));

        var consumer = mock(Consumer.class);
        try (var logs = new LogCapture(KafkaConfig.class.getName())) {
            assertAll(
                    () -> assertThatThrownBy(() -> handler.handleRemaining(
                    new RuntimeException("original"), List.of(consumerRecord), consumer, mock(MessageListenerContainer.class)))
                    .hasMessage("Record in retry and not yet recovered"),
                    () -> assertThat(logs.events()).anySatisfy(event -> {
                assertAll(
                        () -> assertThat(event.getFormattedMessage()).contains("DLT recovery failed", "searchId=key", "dltTopic=test-topic.DLT"),
                        () -> assertThat(event.getThrowableProxy()).isNotNull());
            }));
        }
        assertAll(
                () -> Mockito.verify(kafkaOperations).send(ArgumentMatchers.<ProducerRecord<String, Object>>any()),
                () -> Mockito.verify(consumer).seek(new TopicPartition("test-topic", 0), 0L),
                () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
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
        var factory = kafkaConfig.kafkaConsumerFactory(new KafkaProperties(), applicationMapper(), new SimpleMeterRegistry());
        var deserializer = factory.getValueDeserializer();
        deserializer.configure(factory.getConfigurationProperties(), false);
        byte[] json = ("{\"searchId\":\"strict-id\",\"hotelId\":\"hotel\",\"checkIn\":\"2026-12-29\",\"checkOut\":\"2026-12-31\",\"ages\":" + ages + "}")
                .getBytes(StandardCharsets.UTF_8);
        var headers = new RecordHeaders();
        var decoded = deserializer.deserialize("test-topic", headers, json);
        var header = headers.lastHeader(KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
        assertAll(
                () -> assertThat(decoded).isNull(),
                () -> assertThat(header).isNotNull());
        var failure = SerializationUtils.byteArrayToDeserializationException(
                new LogAccessor(KafkaConfigTest.class), header);
        assertThat(failure.getData()).containsExactly(json);
    }

    @Test
    void configuredSerdesShouldRoundTripValidMessageAndPreserveOrder() {
        var mapper = applicationMapper();
        var kafkaProperties = new KafkaProperties();
        var producerFactory = kafkaConfig.kafkaProducerFactory(kafkaProperties, mapper, new SimpleMeterRegistry());
        var consumerFactory = kafkaConfig.kafkaConsumerFactory(kafkaProperties, mapper, new SimpleMeterRegistry());
        var serializer = producerFactory.getValueSerializer();
        var deserializer = consumerFactory.getValueDeserializer();
        serializer.configure(producerFactory.getConfigurationProperties(), false);
        deserializer.configure(consumerFactory.getConfigurationProperties(), false);
        var message = new SearchKafkaMessage("round-trip", "hotel", "2026-12-29", "2026-12-31", List.of(30, 1, 30));
        var headers = new RecordHeaders();
        byte[] json = serializer.serialize("test-topic", headers, message);
        assertAll(
                () -> assertThat(deserializer.deserialize("test-topic", headers, json)).isEqualTo(message),
                () -> assertThat(serializer.serialize("test-topic", headers, null)).isNull(),
                () -> assertThat(deserializer.deserialize("test-topic", headers, (byte[]) null)).isNull());
    }

    @Test
    void deserializationFailureShouldRecoverOriginalBytesThroughConfiguredSerializer() {
        byte[] original = "{not valid json!!!}".getBytes(StandardCharsets.UTF_8);
        var kafkaProperties = new KafkaProperties();
        var mapper = applicationMapper();
        var consumerFactory = kafkaConfig.kafkaConsumerFactory(kafkaProperties, mapper, new SimpleMeterRegistry());
        var deserializer = consumerFactory.getValueDeserializer();
        deserializer.configure(consumerFactory.getConfigurationProperties(), false);
        var consumerRecord = new ConsumerRecord<String, SearchKafkaMessage>("test-topic", 0, 7L, "bad-key", null);
        consumerRecord.headers().add(CorrelationId.HEADER, "raw-dlt-correlation".getBytes(StandardCharsets.UTF_8));
        assertThat(deserializer.deserialize(consumerRecord.topic(), consumerRecord.headers(), original)).isNull();
        var failure = SerializationUtils.byteArrayToDeserializationException(
                new LogAccessor(KafkaConfigTest.class),
                consumerRecord.headers().lastHeader(KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER));
        var producerFactory = kafkaConfig.kafkaProducerFactory(kafkaProperties, mapper, new SimpleMeterRegistry());
        var serializer = producerFactory.getValueSerializer();
        serializer.configure(producerFactory.getConfigurationProperties(), false);
        when(kafkaOperations.send(ArgumentMatchers.<ProducerRecord<String, Object>>any())).thenAnswer(inv -> {
            ProducerRecord<String, Object> sent = inv.getArgument(0);
            assertAll(
                    () -> assertThat(sent.topic()).isEqualTo("hotel_availability_searches.DLT"),
                    () -> assertThat(sent.key()).isEqualTo("bad-key"),
                    () -> assertThat(sent.value()).isInstanceOf(byte[].class),
                    () -> assertThat(serializer.serialize(sent.topic(), sent.headers(), sent.value())).containsExactly(original),
                    () -> assertThat(new String(sent.headers().lastHeader(CorrelationId.HEADER).value(), StandardCharsets.UTF_8))
                    .isEqualTo("raw-dlt-correlation"));
            return CompletableFuture.completedFuture(new SendResult<>(sent,
                    new RecordMetadata(new TopicPartition(sent.topic(), 0), 0, 0, 0, 0, 0)));
        });
        var handler = kafkaConfig.kafkaErrorHandler(kafkaOperations);
        assertAll(
                () -> assertThat(handler.handleOne(failure, consumerRecord, mock(Consumer.class), mock(MessageListenerContainer.class))).isTrue(),
                () -> Mockito.verify(kafkaOperations).send(ArgumentMatchers.<ProducerRecord<String, Object>>any()),
                () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
    }

    @Test
    void nullMessageShouldRecoverImmediatelyWithoutRetry() {
        var consumerRecord = new ConsumerRecord<String, SearchKafkaMessage>("test-topic", 0, 8, "null-key", null);
        when(kafkaOperations.send(ArgumentMatchers.<ProducerRecord<String, Object>>any())).thenAnswer(inv -> {
            ProducerRecord<String, Object> sent = inv.getArgument(0);
            assertThat(sent.value()).isNull();
            return CompletableFuture.completedFuture(new SendResult<>(sent,
                    new RecordMetadata(new TopicPartition(sent.topic(), 0), 0, 0, 0, 0, 0)));
        });
        var handler = kafkaConfig.kafkaErrorHandler(kafkaOperations);
        long start = System.nanoTime();
        assertAll(
                () -> assertThat(handler.handleOne(new InvalidKafkaMessageException("null message", null), consumerRecord,
                mock(Consumer.class), mock(MessageListenerContainer.class))).isTrue(),
                () -> assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(DEFAULT_PROPS.retry().interval()));
        Mockito.verify(kafkaOperations).send(ArgumentMatchers.<ProducerRecord<String, Object>>any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"record", "batch"})
    void springBootShouldWireConfiguredFactoriesAndStrictMapper(String ackMode) {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        JacksonAutoConfiguration.class,
                        KafkaAutoConfiguration.class))
                .withUserConfiguration(KafkaConfig.class, JacksonConfig.class)
                .withBean(io.micrometer.core.instrument.MeterRegistry.class, SimpleMeterRegistry::new)
                .withPropertyValues(
                        "spring.kafka.admin.auto-create=false",
                        "spring.kafka.producer.acks=all",
                        "spring.kafka.producer.properties.enable.idempotence=true",
                        "spring.kafka.consumer.group-id=configuration-test",
                        "spring.kafka.listener.ack-mode=" + ackMode,
                        "spring.kafka.listener.concurrency=6",
                        "spring.jackson.deserialization.fail-on-unknown-properties=true",
                        "hotel-search.kafka.topic=test-topic", "hotel-search.kafka.dlt-suffix=.DLT",
                        "hotel-search.kafka.retry.interval=5s", "hotel-search.kafka.retry.max-attempts=60",
                        "hotel-search.kafka.producer.send-timeout=10s", "hotel-search.kafka.dlt.send-timeout=10s")
                .run(context -> {
                    assertAll(
                            () -> assertThat(context).hasNotFailed(),
                            () -> assertThat(context).hasSingleBean(SearchKafkaProducer.class),
                            () -> assertThat(context).hasSingleBean(KafkaTemplate.class));
                    var mapper = context.getBean(JsonMapper.class);
                    assertAll(
                            () -> assertThatThrownBy(() -> mapper.readValue("\"30\"", Integer.class))
                            .isInstanceOf(MismatchedInputException.class),
                            () -> assertThatThrownBy(() -> mapper.readValue("30.9", Integer.class))
                            .isInstanceOf(MismatchedInputException.class),
                            () -> assertThatThrownBy(() -> mapper.readValue("30", String.class))
                            .isInstanceOf(MismatchedInputException.class));
                    var producer = context.getBean(DefaultKafkaProducerFactory.class);
                    assertThat(producer.getConfigurationProperties()).containsEntry("acks", "all")
                            .containsEntry("enable.idempotence", "true");
                    ConcurrentKafkaListenerContainerFactory<?, ?> listener = context.getBean("kafkaListenerContainerFactory",
                            ConcurrentKafkaListenerContainerFactory.class);
                    assertThat(listener.getContainerProperties().getAckMode())
                            .isEqualTo(ContainerProperties.AckMode.valueOf(ackMode.toUpperCase(java.util.Locale.ROOT)));
                    assertAll(
                            () -> assertThat(listener.getContainerProperties().isSyncCommits()).isTrue(),
                            () -> assertThat(listener.createContainer("test-topic").getConcurrency()).isEqualTo(6),
                            () -> assertThat(listener.isBatchListener()).isFalse());
                    var executor = (SimpleAsyncTaskExecutor) listener.getContainerProperties().getListenerTaskExecutor();
                    var virtualThread = new CompletableFuture<Boolean>();
                    executor.execute(() -> virtualThread.complete(Thread.currentThread().isVirtual()));
                    assertThat(virtualThread.get(5, TimeUnit.SECONDS)).isTrue();
                });
    }

    @Test
    void configuredPartitionsShouldApplyToSearchAndDltTopics() {
        var config = new KafkaConfig(DEFAULT_PROPS, 3);
        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertThat(config.searchesTopic().numPartitions()).isEqualTo(3),
                () -> assertThat(config.dltTopic().numPartitions()).isEqualTo(3));
    }

    @Test
    void nonPositivePartitionsShouldFailConfiguration() {
        assertThatThrownBy(() -> new KafkaConfig(DEFAULT_PROPS, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Kafka partitions must be positive");
    }

}
