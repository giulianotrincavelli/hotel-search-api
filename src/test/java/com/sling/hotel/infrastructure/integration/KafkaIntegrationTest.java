package com.sling.hotel.infrastructure.integration;

import com.sling.hotel.application.ports.out.SearchRepositoryPort;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.logging.CorrelationId;
import com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage;
import com.sling.hotel.support.LogCapture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 1,
        topics = {
                KafkaIntegrationTest.TEST_TOPIC,
                KafkaIntegrationTest.TEST_DLT_TOPIC
        }
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=kafka-integration",
        "hotel-search.kafka.topic=" + KafkaIntegrationTest.TEST_TOPIC,
        "hotel-search.kafka.dlt-suffix=.DLT",
        "hotel-search.kafka.retry.interval=1s",
        "hotel-search.kafka.retry.max-attempts=60",
        "hotel-search.kafka.producer.send-timeout=10s",
        "hotel-search.kafka.dlt.send-timeout=10s",
        "spring.autoconfigure.exclude=" +
                "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration," +
                "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration," +
                "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"
})
class KafkaIntegrationTest {

    static final String TEST_TOPIC = "hotel_availability_searches";
    static final String TEST_DLT_TOPIC = TEST_TOPIC + ".DLT";

    @MockitoBean
    private SearchRepositoryPort searchRepositoryPort;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private MockMvc mockMvc;

    private Consumer<String, byte[]> dltConsumer;
    private RecordMetadata lastSent;

    @BeforeEach
    void setUp() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "dlt-" + UUID.randomUUID(), false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        dltConsumer = new DefaultKafkaConsumerFactory<String, byte[]>(props).createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(dltConsumer, true, TEST_DLT_TOPIC);
        lastSent = null;
    }

    @AfterEach
    void tearDown() {
        try {
            awaitCommitted();
        } finally {
            if (dltConsumer != null) {
                dltConsumer.close();
            }
        }
    }

    @Test
    void validMessageShouldReachSavePort() throws Exception {
        var message = new SearchKafkaMessage(
                "search-valid", "hotel-1", "2023-12-29", "2023-12-31", List.of(30, 29));

        send(new ProducerRecord<>(TEST_TOPIC, "search-valid", message));
        var captor = ArgumentCaptor.forClass(HotelSearch.class);
        verify(searchRepositoryPort, timeout(10_000)).save(captor.capture());
        assertThat(captor.getValue()).isEqualTo(new HotelSearch("search-valid", "hotel-1",
                java.time.LocalDate.of(2023, 12, 29), java.time.LocalDate.of(2023, 12, 31), List.of(30, 29)));
    }

    @Test
    void httpCorrelationIdShouldReachVirtualThreadConsumerAndBothLogContexts() throws Exception {
        var consumerCorrelation = new AtomicReference<String>();
        var consumerThread = new AtomicReference<Thread>();
        var persistedSearch = new AtomicReference<HotelSearch>();
        doAnswer(inv -> {
            consumerCorrelation.set(MDC.get(CorrelationId.MDC_KEY));
            consumerThread.set(Thread.currentThread());
            persistedSearch.set(inv.getArgument(0));
            return null;
        }).when(searchRepositoryPort).save(any(HotelSearch.class));

        try (var logs = new LogCapture("com.sling.hotel")) {
            var result = mockMvc.perform(post("/search")
                            .header(CorrelationId.HEADER, "http-to-kafka-correlation")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"hotelId\":\"correlation-hotel\",\"checkIn\":\"29/12/2026\",\"checkOut\":\"31/12/2026\",\"ages\":[30,29]}"))
                    .andExpect(status().isOk())
                    .andExpect(header().string(CorrelationId.HEADER, "http-to-kafka-correlation"))
                    .andExpect(jsonPath("$.searchId").isNotEmpty())
                    .andReturn();
            assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(persistedSearch.get()).isNotNull();
                assertThat(consumerCorrelation.get()).isEqualTo("http-to-kafka-correlation");
                assertThat(consumerThread.get().isVirtual()).isTrue();
                assertThat(logs.events()).anySatisfy(event -> {
                    assertThat(event.getFormattedMessage()).contains("Kafka message persisted:");
                    assertThat(event.getMDCPropertyMap()).containsEntry(CorrelationId.MDC_KEY, "http-to-kafka-correlation");
                });
            });
            var end = KafkaTestUtils.getEndOffsets(dltConsumer, TEST_TOPIC, 0).values().iterator().next();
            lastSent = new RecordMetadata(new org.apache.kafka.common.TopicPartition(TEST_TOPIC, 0), end - 1, 0, 0, 0, 0);
            String id = persistedSearch.get().searchId();
            assertThat(id).isNotEqualTo("http-to-kafka-correlation");
            assertThat(result.getResponse().getContentAsString()).contains(id);
            assertThat(logs.events()).anySatisfy(event -> {
                assertThat(event.getFormattedMessage()).contains("HTTP response:", id);
                assertThat(event.getMDCPropertyMap()).containsEntry(CorrelationId.MDC_KEY, "http-to-kafka-correlation");
            }).anySatisfy(event -> {
                assertThat(event.getFormattedMessage()).contains("Kafka publication confirmed:", id);
                assertThat(event.getMDCPropertyMap()).containsEntry(CorrelationId.MDC_KEY, "http-to-kafka-correlation");
            });
        }
    }

    @Test
    void invalidDateShouldBeSentToDltWithoutRetries() throws Exception {
        var message = new SearchKafkaMessage(
                "search-invalid", "hotel-1", "not-a-date", "2023-12-31", List.of(30));

        var record = new ProducerRecord<String, Object>(TEST_TOPIC, "search-invalid", message);
        record.headers().add(CorrelationId.HEADER, "invalid-message-correlation".getBytes(StandardCharsets.UTF_8));
        send(record);

        var recovered = dltRecord("search-invalid");
        assertThat(new String(recovered.value(), StandardCharsets.UTF_8)).contains("not-a-date", "search-invalid");
        assertThat(new String(recovered.headers().lastHeader(CorrelationId.HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("invalid-message-correlation");
        Mockito.verifyNoInteractions(searchRepositoryPort);
    }

    @Test
    void nullFieldsShouldBeSentToDltWithoutRetries() throws Exception {
        var message = new SearchKafkaMessage(
                null, "hotel-1", "2023-12-29", "2023-12-31", List.of(30));

        send(new ProducerRecord<>(TEST_TOPIC, "null-field", message));

        var recovered = dltRecord("null-field");
        assertThat(new String(recovered.value(), StandardCharsets.UTF_8)).contains("\"searchId\":null");
        Mockito.verifyNoInteractions(searchRepositoryPort);
    }

    @Test
    void transientErrorShouldRetryTwiceAndCommitAfterSuccess() throws Exception {
        var callCount = new AtomicInteger(0);
        var correlations = new CopyOnWriteArrayList<String>();
        var attempts = new CopyOnWriteArrayList<Long>();
        doAnswer(invocation -> {
            attempts.add(System.nanoTime());
            correlations.add(MDC.get(CorrelationId.MDC_KEY));
            if (callCount.incrementAndGet() <= 2) {
                throw new RuntimeException("Oracle temporarily unavailable");
            }
            return null;
        }).when(searchRepositoryPort).save(any(HotelSearch.class));

        var message = new SearchKafkaMessage(
                "search-retry", "hotel-1", "2023-12-29", "2023-12-31", List.of(25));

        var record = new ProducerRecord<String, Object>(TEST_TOPIC, "search-retry", message);
        record.headers().add(CorrelationId.HEADER, "retry-correlation".getBytes(StandardCharsets.UTF_8));
        send(record);

        awaitCommitted();
        verify(searchRepositoryPort, Mockito.times(3)).save(any(HotelSearch.class));
        assertThat(callCount.get()).isEqualTo(3);
        assertThat(correlations).containsExactly("retry-correlation", "retry-correlation", "retry-correlation");
        assertThat(Duration.ofNanos(attempts.get(1) - attempts.get(0))).isGreaterThanOrEqualTo(Duration.ofSeconds(1));
        assertThat(Duration.ofNanos(attempts.get(2) - attempts.get(1))).isGreaterThanOrEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void kafkaListenerShouldExecuteOnVirtualThread() throws Exception {
        var threadRef = new AtomicReference<Thread>();
        doAnswer(inv -> {
            threadRef.set(Thread.currentThread());
            return null;
        }).when(searchRepositoryPort).save(any(HotelSearch.class));

        var message = new SearchKafkaMessage(
                "vt-test", "hotel-1", "2023-12-29", "2023-12-31", List.of(30));
        send(new ProducerRecord<>(TEST_TOPIC, "vt-test", message));

        verify(searchRepositoryPort, timeout(10_000)).save(any(HotelSearch.class));
        assertThat(threadRef.get()).isNotNull();
        assertThat(threadRef.get().isVirtual()).isTrue();
    }

    @Test
    void kafkaListenerShouldPreserveAgesOrderAndDuplicates() throws Exception {
        doNothing().when(searchRepositoryPort).save(any(HotelSearch.class));

        var message = new SearchKafkaMessage(
                "order-test", "hotel-1", "2023-12-29", "2023-12-31", List.of(5, 5, 30, 1));
        send(new ProducerRecord<>(TEST_TOPIC, "order-test", message));

        var captor = ArgumentCaptor.forClass(HotelSearch.class);
        verify(searchRepositoryPort, timeout(10_000)).save(captor.capture());
        assertThat(captor.getValue().ages()).containsExactly(5, 5, 30, 1);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{not valid json!!!}",
            "{\"searchId\":\"strict\",\"hotelId\":\"hotel\",\"checkIn\":\"2026-12-29\",\"checkOut\":\"2026-12-31\",\"ages\":[\"30\"]}",
            "{\"searchId\":\"strict\",\"hotelId\":\"hotel\",\"checkIn\":\"2026-12-29\",\"checkOut\":\"2026-12-31\",\"ages\":[30.9]}"
    })
    void deserializationFailureShouldRetainOriginalBytesInDlt(String json) throws Exception {
        byte[] original = json.getBytes(StandardCharsets.UTF_8);
        Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafka);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        String key = "bad-" + UUID.randomUUID();
        var record = new ProducerRecord<>(TEST_TOPIC, key, original);
        record.headers().add(CorrelationId.HEADER, "deserialization-correlation".getBytes(StandardCharsets.UTF_8));
        var factory = new DefaultKafkaProducerFactory<String, byte[]>(props);
        try (var producer = factory.createProducer()) {
            lastSent = producer.send(record).get(10, TimeUnit.SECONDS);
        } finally {
            factory.destroy();
        }
        var recovered = dltRecord(key);
        assertThat(recovered.value()).containsExactly(original);
        assertThat(new String(recovered.headers().lastHeader(CorrelationId.HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo("deserialization-correlation");
        Mockito.verifyNoInteractions(searchRepositoryPort);
    }

    @Test
    void nullValueShouldReachDltWithoutRetriesAndAllowNextMessage() throws Exception {
        send(new ProducerRecord<>(TEST_TOPIC, "null-value", null));
        assertThat(dltRecord("null-value").value()).isNull();
        awaitCommitted();
        Mockito.verifyNoInteractions(searchRepositoryPort);
        var valid = new SearchKafkaMessage("after-null", "hotel", "2026-12-29", "2026-12-31", List.of(30));
        send(new ProducerRecord<>(TEST_TOPIC, valid.searchId(), valid));
        awaitCommitted();
        verify(searchRepositoryPort).save(ArgumentMatchers.argThat(search -> search.searchId().equals("after-null")));
    }

    private void send(ProducerRecord<String, Object> record) throws Exception {
        lastSent = kafkaTemplate.send(record).get(10, TimeUnit.SECONDS).getRecordMetadata();
    }

    private ConsumerRecord<String, byte[]> dltRecord(String key) {
        var found = new AtomicReference<ConsumerRecord<String, byte[]>>();
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            for (var record : dltConsumer.poll(Duration.ofMillis(200))) {
                if (key.equals(record.key())) {
                    found.set(record);
                }
            }
            return found.get() != null;
        });
        return found.get();
    }

    private void awaitCommitted() {
        if (lastSent == null) {
            return;
        }
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var offset = KafkaTestUtils.getCurrentOffset(embeddedKafka.getBrokersAsString(),
                    "kafka-integration", lastSent.topic(), lastSent.partition());
            assertThat(offset).isNotNull();
            assertThat(offset.offset()).isGreaterThan(lastSent.offset());
        });
    }
}
