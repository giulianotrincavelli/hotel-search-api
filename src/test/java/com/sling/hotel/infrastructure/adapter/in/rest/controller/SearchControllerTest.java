package com.sling.hotel.infrastructure.adapter.in.rest.controller;

import com.sling.hotel.application.exception.PublishException;
import com.sling.hotel.application.model.CreateSearchInput;
import com.sling.hotel.application.ports.in.CountSearchPort;
import com.sling.hotel.application.ports.in.CreateSearchPort;
import com.sling.hotel.application.usecase.CreateSearchUseCase;
import com.sling.hotel.application.usecase.SaveSearchUseCase;
import com.sling.hotel.domain.exception.SearchNotFoundException;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.adapter.in.kafka.SearchKafkaConsumer;
import com.sling.hotel.infrastructure.adapter.out.kafka.SearchKafkaProducer;
import com.sling.hotel.infrastructure.adapter.out.persistence.SearchPersistenceAdapter;
import com.sling.hotel.infrastructure.adapter.out.persistence.repository.SearchJpaRepository;
import com.sling.hotel.infrastructure.config.JacksonConfig;
import com.sling.hotel.infrastructure.config.ValidationConfig;
import org.springframework.test.context.bean.override.convention.TestBean;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import com.sling.hotel.infrastructure.logging.CorrelationId;
import com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage;
import com.sling.hotel.support.LogCapture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.json.JsonCompareMode;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(SearchController.class)
@Import({JacksonConfig.class, ValidationConfig.class})
class SearchControllerTest {

    @TestBean
    private Clock clock;

    static Clock clock() {
        return Clock.fixed(Instant.parse("2026-10-13T12:00:00Z"), ZoneOffset.UTC);
    }

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CreateSearchPort createSearchPort;

    @MockitoBean
    private CountSearchPort countSearchPort;

    @BeforeEach
    void setUpDefaultMock() {
        when(createSearchPort.create(any(CreateSearchInput.class)))
                .thenAnswer(inv -> {
                    CreateSearchInput cmd = inv.getArgument(0);
                    return new HotelSearch(UUID.randomUUID().toString(),
                            cmd.hotelId(), cmd.checkIn(), cmd.checkOut(), cmd.ages());
                });
    }

    private static final String VALID_REQUEST = """
            {
              "hotelId": "1234aBc",
              "checkIn": "29/12/2030",
              "checkOut": "31/12/2030",
              "ages": [30, 29, 1, 3]
            }
            """;

    @Test
    void postSearch_validRequest_returnsSearchId() throws Exception {
        when(createSearchPort.create(any(CreateSearchInput.class)))
                .thenAnswer(inv -> {
                    CreateSearchInput cmd = inv.getArgument(0);
                    return new HotelSearch("generated-id", cmd.hotelId(), cmd.checkIn(), cmd.checkOut(), cmd.ages());
                });

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.searchId").value("generated-id"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void correlationShouldFollowHttpProducerVirtualConsumerAndPersistenceWithSimulatedTransport() throws Exception {
        KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
        var sent = new AtomicReference<ProducerRecord<String, Object>>();
        when(template.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenAnswer(inv -> {
                    ProducerRecord<String, Object> consumerRecord = inv.getArgument(0);
                    sent.set(consumerRecord);
                    return CompletableFuture.completedFuture(new SendResult<>(consumerRecord,
                            new RecordMetadata(new TopicPartition(consumerRecord.topic(), 0), 42L, 0, 0, 0, 0)));
                });
        var create = new CreateSearchUseCase(new SearchKafkaProducer(template, "hotel_availability_searches", Duration.ofSeconds(10)));
        when(createSearchPort.create(any(CreateSearchInput.class))).thenAnswer(inv -> create.create(inv.getArgument(0)));
        var repository = mock(SearchJpaRepository.class);
        when(repository.insertSearch(any(), any(), any(), any(), any())).thenAnswer(inv -> {
            assertAll(
                    () -> assertThat(Thread.currentThread().isVirtual()).isTrue(),
                    () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isEqualTo("full-flow-correlation"));
            return 1;
        });
        var consumer = new SearchKafkaConsumer(new SaveSearchUseCase(new SearchPersistenceAdapter(repository)));

        try (var logs = new LogCapture("com.sling.hotel")) {
            var result = mockMvc.perform(post("/search").contentType(MediaType.APPLICATION_JSON)
                            .header(CorrelationId.HEADER, "full-flow-correlation").content(VALID_REQUEST))
                    .andExpect(status().isAccepted())
                    .andExpect(header().string(CorrelationId.HEADER, "full-flow-correlation"))
                    .andReturn();
            assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
            var produced = sent.get();
            assertThat(new String(produced.headers().lastHeader(CorrelationId.HEADER).value(), StandardCharsets.UTF_8))
                    .isEqualTo("full-flow-correlation");
            var received = new ConsumerRecord<>(produced.topic(), 0, 42L, produced.key(), (SearchKafkaMessage) produced.value());
            produced.headers().forEach(h -> received.headers().add(h));
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                executor.submit(() -> {
                    assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
                    consumer.consume(received);
                    assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
                }).get(5, TimeUnit.SECONDS);
            }
            assertAll(
                    () -> assertThat(result.getResponse().getContentAsString()).contains(produced.key()),
                    () -> verify(repository).insertSearch(produced.key(), "1234aBc", LocalDate.of(2030, 12, 29),
                            LocalDate.of(2030, 12, 31), "30,29,1,3"));
            for (String phase : List.of("HTTP request:", "HTTP response:", "Kafka publication confirmed:",
                    "Kafka message received:", "Search inserted:", "Kafka message persisted:")) {
                assertThat(logs.events()).anySatisfy(event -> {
                    assertAll(
                            () -> assertThat(event.getFormattedMessage()).contains(phase),
                            () -> assertThat(event.getMDCPropertyMap()).containsEntry(CorrelationId.MDC_KEY, "full-flow-correlation"));
                });
            }
        }
    }

    @Test
    void postSearch_validRequest_parsesFieldsCorrectly() throws Exception {
        when(createSearchPort.create(any(CreateSearchInput.class)))
                .thenAnswer(inv -> {
                    CreateSearchInput cmd = inv.getArgument(0);
                    return new HotelSearch("id-1", cmd.hotelId(), cmd.checkIn(), cmd.checkOut(), cmd.ages());
                });

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isAccepted());

        verify(createSearchPort).create(argThat(cmd ->
                cmd.hotelId().equals("1234aBc")
                        && cmd.checkIn().equals(LocalDate.of(2030, 12, 29))
                        && cmd.checkOut().equals(LocalDate.of(2030, 12, 31))
                        && cmd.ages().equals(List.of(30, 29, 1, 3))
        ));
    }

    @Test
    void postSearch_missingHotelId_returns400() throws Exception {
        var body = """
                {
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [30]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors", hasSize(1)))
                .andExpect(jsonPath("$.errors[0]", containsString("hotelId")));
    }

    @Test
    void postSearch_invalidCheckInFormat_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "2030-12-29",
                  "checkOut": "31/12/2030",
                  "ages": [30]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message", containsString("checkIn")));
    }

    @Test
    void postSearch_invalidCheckOutFormat_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "invalid",
                  "ages": [30]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message", containsString("checkOut")));
    }

    @Test
    void postSearch_checkInAfterCheckOut_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "31/12/2030",
                  "checkOut": "29/12/2030",
                  "ages": [30]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message", containsString("checkOut must be after checkIn")));
    }

    @Test
    void postSearch_checkInEqualsCheckOut_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "29/12/2030",
                  "ages": [30]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message", containsString("checkOut must be after checkIn")));
    }

    @Test
    void postSearch_emptyAges_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": []
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]", containsString("ages")));
    }

    @Test
    void postSearch_negativeAge_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [30, -1]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]", containsString("age")));
    }

    @Test
    void postSearch_nullAge_returns400WithMessage() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [30, null]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void postSearch_kafkaFailure_returns503() throws Exception {
        when(createSearchPort.create(any(CreateSearchInput.class)))
                .thenThrow(new PublishException("Kafka down", new RuntimeException()));

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value(503))
                .andExpect(jsonPath("$.message", containsString("temporarily unavailable")));
    }

    @Test
    void postSearch_unknownField_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [30],
                  "unknownField": "value"
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("{\"status\":400,\"message\":\"Unknown property: unknownField\",\"errors\":null}", JsonCompareMode.STRICT));
    }

    @Test
    void getCount_validSearchId_returnsCountResponse() throws Exception {
        var search = new HotelSearch(
                "search-1", "1234aBc",
                LocalDate.of(2030, 12, 29), LocalDate.of(2030, 12, 31),
                List.of(30, 29, 1, 3)
        );
        when(countSearchPort.count("search-1"))
                .thenReturn(new CountSearchPort.Result(search, 100));

        mockMvc.perform(get("/count").param("searchId", "search-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.searchId").value("search-1"))
                .andExpect(jsonPath("$.search.hotelId").value("1234aBc"))
                .andExpect(jsonPath("$.search.checkIn").value("29/12/2030"))
                .andExpect(jsonPath("$.search.checkOut").value("31/12/2030"))
                .andExpect(jsonPath("$.search.ages", hasSize(4)))
                .andExpect(jsonPath("$.count").value(100));
    }

    @Test
    void getCount_missingSearchId_returns400() throws Exception {
        mockMvc.perform(get("/count"))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("{\"status\":400,\"message\":\"searchId is required\",\"errors\":null}", JsonCompareMode.STRICT));
    }

    @Test
    void unsupportedMethodShouldKeep405AndCommonSchema() throws Exception {
        mockMvc.perform(put("/search"))
                .andExpect(status().isMethodNotAllowed())
                .andExpect(header().string("Allow", containsString("POST")))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", aMapWithSize(3)))
                .andExpect(jsonPath("$.status").value(405))
                .andExpect(jsonPath("$.message", notNullValue()))
                .andExpect(jsonPath("$.errors", nullValue()));
    }

    @Test
    void unsupportedContentTypeShouldKeep415AndCommonSchema() throws Exception {
        mockMvc.perform(post("/search").contentType(MediaType.TEXT_PLAIN).content(VALID_REQUEST))
                .andExpect(status().isUnsupportedMediaType())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", aMapWithSize(3)))
                .andExpect(jsonPath("$.status").value(415))
                .andExpect(jsonPath("$.message", notNullValue()))
                .andExpect(jsonPath("$.errors", nullValue()));
    }

    @Test
    void unsupportedAcceptShouldKeep406AndCommonSchema() throws Exception {
        mockMvc.perform(post("/search").contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.TEXT_PLAIN).content(VALID_REQUEST))
                .andExpect(status().isNotAcceptable())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$", aMapWithSize(3)))
                .andExpect(jsonPath("$.status").value(406))
                .andExpect(jsonPath("$.message", notNullValue()))
                .andExpect(jsonPath("$.errors", nullValue()));
    }

    @Test
    void getCount_notFound_returns404() throws Exception {
        when(countSearchPort.count("missing"))
                .thenThrow(new SearchNotFoundException("missing"));

        mockMvc.perform(get("/count").param("searchId", "missing"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.message", containsString("missing")));
    }

    @Test
    void postSearch_malformedJson_returns400() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{invalid json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message", containsString("Malformed")));
    }

    @Test
    void postSearch_hotelIdExceeds50Chars_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "%s",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [30]
                }
                """.formatted("a".repeat(51));

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]", containsString("hotelId")));
    }

    @Test
    void postSearch_hotelIdExactly50Chars_isAccepted() throws Exception {
        when(createSearchPort.create(any(CreateSearchInput.class)))
                .thenAnswer(inv -> {
                    CreateSearchInput cmd = inv.getArgument(0);
                    return new HotelSearch("id-1", cmd.hotelId(), cmd.checkIn(), cmd.checkOut(), cmd.ages());
                });

        var body = """
                {
                  "hotelId": "%s",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [30]
                }
                """.formatted("a".repeat(50));

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted());
    }

    @Test
    void postSearch_missingAges_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030"
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]", containsString("ages")));
    }

    @Test
    void postSearch_missingCheckIn_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkOut": "31/12/2030",
                  "ages": [30]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]", containsString("checkIn")));
    }

    @Test
    void postSearch_missingCheckOut_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "ages": [30]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]", containsString("checkOut")));
    }

    @Test
    void postSearch_agesExceeds50Elements_returns400() throws Exception {
        var ages = java.util.stream.IntStream.range(0, 51)
                .mapToObj(String::valueOf)
                .collect(java.util.stream.Collectors.joining(","));
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [%s]
                }
                """.formatted(ages);

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]", containsString("ages")));
    }

    @Test
    void postSearch_hotelIdBlank_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "   ",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [30]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]", containsString("hotelId")));
    }

    @Test
    void postSearch_internalError_returns500() throws Exception {
        when(createSearchPort.create(any(CreateSearchInput.class)))
                .thenThrow(new RuntimeException("unexpected"));

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(VALID_REQUEST))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.status").value(500))
                .andExpect(jsonPath("$.message", containsString("Internal server error")));
    }

    @Test
    void getCount_blankSearchId_returns400() throws Exception {
        mockMvc.perform(get("/count").param("searchId", ""))
                .andExpect(status().isBadRequest());
    }

    @Test
    void getCount_whitespaceSearchId_returns400() throws Exception {
        mockMvc.perform(get("/count").param("searchId", "   "))
                .andExpect(status().isBadRequest());
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"\"thirty\"", "\"30\"", "30.5", "true"})
    void postSearch_invalidAgeType_returns400(String age) throws Exception {
        String body = "{\"hotelId\":\"h1\",\"checkIn\":\"29/12/2030\",\"checkOut\":\"31/12/2030\",\"ages\":[%s]}".formatted(age);
        mockMvc.perform(post("/search").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message", containsString("ages")))
                .andExpect(jsonPath("$.message", containsString("integer")));
    }

    @Test
    void postSearch_ageAbove100_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [30, 101]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]", containsString("age")));
    }

    @Test
    void postSearch_age100_isAccepted() throws Exception {
        when(createSearchPort.create(any(CreateSearchInput.class)))
                .thenAnswer(inv -> {
                    CreateSearchInput cmd = inv.getArgument(0);
                    return new HotelSearch("id-1", cmd.hotelId(), cmd.checkIn(), cmd.checkOut(), cmd.ages());
                });

        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [0, 100]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted());
    }

    @Test
    void postSearch_sqlInjectionInHotelId_returns202NotError() throws Exception {
        when(createSearchPort.create(any(CreateSearchInput.class)))
                .thenAnswer(inv -> {
                    CreateSearchInput cmd = inv.getArgument(0);
                    return new HotelSearch("id-1", cmd.hotelId(), cmd.checkIn(), cmd.checkOut(), cmd.ages());
                });

        var body = """
                {
                  "hotelId": "'; DROP TABLE hotel_search;--",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [30]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.searchId").exists());

        verify(createSearchPort).create(argThat(cmd ->
                cmd.hotelId().equals("'; DROP TABLE hotel_search;--")));
    }

    @Test
    void postSearch_ageAsStringNumber_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": ["30"]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    void postSearch_50Ages_isAccepted() throws Exception {
        when(createSearchPort.create(any(CreateSearchInput.class)))
                .thenAnswer(inv -> {
                    CreateSearchInput cmd = inv.getArgument(0);
                    return new HotelSearch("id-1", cmd.hotelId(), cmd.checkIn(), cmd.checkOut(), cmd.ages());
                });

        var ages = java.util.stream.IntStream.range(0, 50)
                .mapToObj(i -> "100")
                .collect(java.util.stream.Collectors.joining(","));
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [%s]
                }
                """.formatted(ages);

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted());
    }

    @Test
    void postSearch_repeatedAges_isAccepted() throws Exception {
        when(createSearchPort.create(any(CreateSearchInput.class)))
                .thenAnswer(inv -> {
                    CreateSearchInput cmd = inv.getArgument(0);
                    return new HotelSearch("id-1", cmd.hotelId(), cmd.checkIn(), cmd.checkOut(), cmd.ages());
                });

        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [5, 5, 5, 5]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted());

        verify(createSearchPort).create(argThat(cmd ->
                cmd.ages().equals(List.of(5, 5, 5, 5))));
    }

    @Test
    void postSearch_agesWithSpaces_isAccepted() throws Exception {
        when(createSearchPort.create(any(CreateSearchInput.class)))
                .thenAnswer(inv -> {
                    CreateSearchInput cmd = inv.getArgument(0);
                    return new HotelSearch("id-1", cmd.hotelId(), cmd.checkIn(), cmd.checkOut(), cmd.ages());
                });

        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": [ 30 , 29 , 1 , 3 ]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted());

        verify(createSearchPort).create(argThat(cmd ->
                cmd.ages().equals(List.of(30, 29, 1, 3))));
    }

    @Test
    void postSearch_nonStrictDate_returns400() throws Exception {
        var body = """
                {
                  "hotelId": "1234aBc",
                  "checkIn": "31/02/2030",
                  "checkOut": "31/12/2030",
                  "ages": [30]
                }
                """;

        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message", containsString("checkIn")));
    }

    @Test
    void kafkaInfrastructureFailureShouldReturn503AndProgrammingErrorShouldReturn500() throws Exception {
        KafkaTemplate<String, Object> template = mock();
        var producer = new SearchKafkaProducer(template, "test-topic", Duration.ofSeconds(1));
        var useCase = new CreateSearchUseCase(producer);
        when(createSearchPort.create(any())).thenAnswer(inv -> useCase.create(inv.getArgument(0)));
        when(template.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenThrow(new TimeoutException("Broker unavailable"))
                .thenReturn(CompletableFuture.failedFuture(new NetworkException("Connection lost")))
                .thenThrow(new IllegalArgumentException("Invalid producer configuration"))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Invalid state")));
        for (int expected : List.of(503, 503, 500, 500)) {
            mockMvc.perform(post("/search").contentType(MediaType.APPLICATION_JSON).content(VALID_REQUEST))
                    .andExpect(status().is(expected))
                    .andExpect(jsonPath("$.status").value(expected))
                    .andExpect(jsonPath("$.message").isNotEmpty())
                    .andExpect(jsonPath("$.errors").value(nullValue()));
        }
    }

    @Test
    void dateValidationShouldNotPreventReadingPreviouslyPersistedSearches() throws Exception {
        var historic = new HotelSearch("historic", "hotel", LocalDate.of(2020, 1, 1), LocalDate.of(2020, 1, 2), List.of(30));
        when(countSearchPort.count("historic")).thenReturn(new CountSearchPort.Result(historic, 1));
        mockMvc.perform(get("/count").param("searchId", "historic"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.search.checkIn").value("01/01/2020"));
    }

    @Test
    void kafkaFailureShouldBeLoggedOnceWithPublicationContextAndCause() throws Exception {
        KafkaTemplate<String, Object> template = mock();
        var failure = new org.apache.kafka.common.errors.NetworkException("Connection lost");
        when(template.send(ArgumentMatchers.<ProducerRecord<String, Object>>any()))
                .thenReturn(CompletableFuture.failedFuture(failure));
        var useCase = new CreateSearchUseCase(new SearchKafkaProducer(template, "test-topic", Duration.ofSeconds(1)));
        when(createSearchPort.create(any())).thenAnswer(inv -> useCase.create(inv.getArgument(0)));
        try (var logs = new LogCapture("com.sling.hotel")) {
            mockMvc.perform(post("/search").header(CorrelationId.HEADER, "failed-publication")
                            .contentType(MediaType.APPLICATION_JSON).content(VALID_REQUEST))
                    .andExpect(status().isServiceUnavailable());
            var errors = logs.events().stream().filter(event -> event.getLevel() == ch.qos.logback.classic.Level.ERROR).toList();
            assertThat(errors).singleElement().satisfies(event -> {
                assertAll(
                        () -> assertThat(event.getFormattedMessage()).isEqualTo("Message publish failed"),
                        () -> assertThat(event.getMDCPropertyMap()).containsEntry(CorrelationId.MDC_KEY, "failed-publication"),
                        () -> assertThat(event.getThrowableProxy().getMessage())
                                .contains("searchId=", "topic=test-topic", "correlationId=failed-publication"),
                        () -> assertThat(event.getThrowableProxy().getCause().getMessage()).isEqualTo("Connection lost"));
            });
        }
    }

}
