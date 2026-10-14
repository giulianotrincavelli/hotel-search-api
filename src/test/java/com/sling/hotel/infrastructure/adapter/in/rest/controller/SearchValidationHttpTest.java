package com.sling.hotel.infrastructure.adapter.in.rest.controller;

import com.sling.hotel.application.model.CreateSearchInput;
import com.sling.hotel.application.ports.in.CountSearchPort;
import com.sling.hotel.application.ports.in.CreateSearchPort;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.config.JacksonConfig;
import com.sling.hotel.infrastructure.config.ValidationConfig;
import org.springframework.test.context.bean.override.convention.TestBean;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.json.JsonCompareMode;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(SearchController.class)
@Import({JacksonConfig.class, ValidationConfig.class})
@TestPropertySource(properties = "logging.level.com.sling.hotel.infrastructure.adapter.in.rest.filter.RequestLoggingFilter=DEBUG")
class SearchValidationHttpTest {

    @TestBean
    private Clock clock;

    static Clock clock() {
        return Clock.fixed(Instant.parse("2026-10-13T12:00:00Z"), ZoneOffset.UTC);
    }

    @Test
    void unknownPropertyShouldUseCommonErrorResponse() throws Exception {
        mockMvc.perform(post("/search").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"hotelId\":\"h1\",\"checkIn\":\"29/12/2026\",\"checkOut\":\"31/12/2026\",\"ages\":[30],\"unexpected\":true}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(content().json("{\"status\":400,\"message\":\"Unknown property: unexpected\",\"errors\":null}", JsonCompareMode.STRICT));
        verify(createSearchPort, never()).create(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "null", "[]", "\"text\"", "{", "{\"hotelId\":", "{invalid json"})
    void unreadableBodiesShouldUseCommonErrorResponse(String body) throws Exception {
        mockMvc.perform(post("/search").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(content().json("{\"status\":400,\"message\":\"Malformed request body\",\"errors\":null}", JsonCompareMode.STRICT));
        verify(createSearchPort, never()).create(any());
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

    static Stream<Arguments> hotelIdInvalidCases() {
        return Stream.of(
                Arguments.of("absent",
                        """
                        {"checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30]}
                        """,
                        "hotelId: hotelId is required"),
                Arguments.of("null",
                        """
                        {"hotelId": null, "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30]}
                        """,
                        "hotelId: hotelId is required"),
                Arguments.of("empty",
                        """
                        {"hotelId": "", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30]}
                        """,
                        "hotelId: hotelId is required"),
                Arguments.of("blank",
                        """
                        {"hotelId": "   ", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30]}
                        """,
                        "hotelId: hotelId is required"),
                Arguments.of("51 characters",
                        """
                        {"hotelId": "%s", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30]}
                        """.formatted("a".repeat(51)),
                        "hotelId: hotelId must have at most 50 characters")
        );
    }

    @ParameterizedTest(name = "{0} → 400")
    @MethodSource({"hotelIdInvalidCases", "checkInInvalidCases", "checkOutInvalidCases", "agesTopLevelInvalidCases", "agesElementBeanValidationCases"})
    void invalidFieldsShouldReturnValidationErrors(String caseName, String body, String expectedError) throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", is("Validation failed")))
                .andExpect(jsonPath("$.errors[0]", is(expectedError)));

        verify(createSearchPort, never()).create(any());
    }

    static Stream<Arguments> hotelIdTypeMismatchCases() {
        return Stream.of(
                Arguments.of("number", """
                        {"hotelId": 123, "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30]}
                        """, "hotelId must be a string"),
                Arguments.of("boolean", """
                        {"hotelId": true, "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30]}
                        """, "hotelId must be a string"),
                Arguments.of("array", """
                        {"hotelId": [1], "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30]}
                        """, "hotelId must be a string"),
                Arguments.of("object", """
                        {"hotelId": {"x":1}, "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30]}
                        """, "hotelId must be a string")
        );
    }

    @ParameterizedTest(name = "hotelId as {0} → 400")
    @MethodSource("hotelIdTypeMismatchCases")
    void hotelId_typeMismatch(String caseName, String body, String expectedMessage) throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", is(expectedMessage)));

        verify(createSearchPort, never()).create(any());
    }

    static Stream<Arguments> checkInInvalidCases() {
        return Stream.of(
                Arguments.of("absent",
                        """
                        {"hotelId": "h1", "checkOut": "31/12/2030", "ages": [30]}
                        """,
                        "checkIn: checkIn is required"),
                Arguments.of("null",
                        """
                        {"hotelId": "h1", "checkIn": null, "checkOut": "31/12/2030", "ages": [30]}
                        """,
                        "checkIn: checkIn is required"),
                Arguments.of("empty string",
                        """
                        {"hotelId": "h1", "checkIn": "", "checkOut": "31/12/2030", "ages": [30]}
                        """,
                        "checkIn: checkIn is required")
        );
    }

    static Stream<Arguments> checkInFormatCases() {
        return Stream.of(
                Arguments.of("wrong format", """
                        {"hotelId": "h1", "checkIn": "2030-12-29", "checkOut": "31/12/2030", "ages": [30]}
                        """),
                Arguments.of("nonexistent date", """
                        {"hotelId": "h1", "checkIn": "31/02/2030", "checkOut": "31/12/2030", "ages": [30]}
                        """),
                Arguments.of("number", """
                        {"hotelId": "h1", "checkIn": 123, "checkOut": "31/12/2030", "ages": [30]}
                        """),
                Arguments.of("boolean", """
                        {"hotelId": "h1", "checkIn": true, "checkOut": "31/12/2030", "ages": [30]}
                        """),
                Arguments.of("array", """
                        {"hotelId": "h1", "checkIn": [1], "checkOut": "31/12/2030", "ages": [30]}
                        """),
                Arguments.of("object", """
                        {"hotelId": "h1", "checkIn": {"x":1}, "checkOut": "31/12/2030", "ages": [30]}
                        """)
        );
    }

    @ParameterizedTest(name = "checkIn {0} → 400 with date format message")
    @MethodSource("checkInFormatCases")
    void checkIn_formatAndTypeMismatch(String caseName, String body) throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", is("checkIn must be a valid date in dd/MM/yyyy format")));

        verify(createSearchPort, never()).create(any());
    }

    static Stream<Arguments> checkOutInvalidCases() {
        return Stream.of(
                Arguments.of("absent",
                        """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "ages": [30]}
                        """,
                        "checkOut: checkOut is required"),
                Arguments.of("null",
                        """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": null, "ages": [30]}
                        """,
                        "checkOut: checkOut is required"),
                Arguments.of("empty string",
                        """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "", "ages": [30]}
                        """,
                        "checkOut: checkOut is required")
        );
    }

    static Stream<Arguments> checkOutFormatCases() {
        return Stream.of(
                Arguments.of("wrong format", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "invalid", "ages": [30]}
                        """),
                Arguments.of("nonexistent date", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/02/2030", "ages": [30]}
                        """),
                Arguments.of("number", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": 456, "ages": [30]}
                        """),
                Arguments.of("boolean", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": false, "ages": [30]}
                        """),
                Arguments.of("array", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": [1], "ages": [30]}
                        """),
                Arguments.of("object", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": {"x":1}, "ages": [30]}
                        """)
        );
    }

    @ParameterizedTest(name = "checkOut {0} → 400 with date format message")
    @MethodSource("checkOutFormatCases")
    void checkOut_formatAndTypeMismatch(String caseName, String body) throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", is("checkOut must be a valid date in dd/MM/yyyy format")));

        verify(createSearchPort, never()).create(any());
    }

    @Test
    void checkInEqualsCheckOut_returns400WithDomainMessage() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "29/12/2030", "ages": [30]}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", is("checkOut must be after checkIn")));

        verify(createSearchPort).create(any());
    }

    @Test
    void checkInAfterCheckOut_returns400WithDomainMessage() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "31/12/2030", "checkOut": "29/12/2030", "ages": [30]}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", is("checkOut must be after checkIn")));

        verify(createSearchPort).create(any());
    }

    static Stream<Arguments> agesTopLevelInvalidCases() {
        return Stream.of(
                Arguments.of("absent",
                        """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030"}
                        """,
                        "ages: ages is required and must not be empty"),
                Arguments.of("null",
                        """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": null}
                        """,
                        "ages: ages is required and must not be empty"),
                Arguments.of("empty array",
                        """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": []}
                        """,
                        "ages: ages is required and must not be empty")
        );
    }

    @Test
    void ages_51elements_returns400() throws Exception {
        var ages = IntStream.range(0, 51).mapToObj(String::valueOf).collect(Collectors.joining(","));
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [%s]}
                                """.formatted(ages)))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", is("Validation failed")))
                .andExpect(jsonPath("$.errors[0]", is("ages: ages must have at most 50 elements")));

        verify(createSearchPort, never()).create(any());
    }

    static Stream<Arguments> agesTypeMismatchCases() {
        return Stream.of(
                Arguments.of("string", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": "test"}
                        """),
                Arguments.of("number", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": 123}
                        """),
                Arguments.of("boolean", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": true}
                        """),
                Arguments.of("object", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": {"x":1}}
                        """)
        );
    }

    @ParameterizedTest(name = "{0} → 400")
    @MethodSource({"agesTypeMismatchCases", "bodyInvalidCases"})
    void malformedBodiesShouldReturn400(String caseName, String body) throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", is("Malformed request body")));

        verify(createSearchPort, never()).create(any());
    }

    static Stream<Arguments> agesElementBeanValidationCases() {
        return Stream.of(
                Arguments.of("null", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [null]}
                        """,
                        "ages[0]: each age must not be null"),
                Arguments.of("negative", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [-1]}
                        """,
                        "ages[0]: each age must be an integer between 0 and 100"),
                Arguments.of("above 100", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [101]}
                        """,
                        "ages[0]: each age must be an integer between 0 and 100"),
                Arguments.of("negative at index 1", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30, -5]}
                        """,
                        "ages[1]: each age must be an integer between 0 and 100")
        );
    }

    static Stream<Arguments> agesElementTypeMismatchCases() {
        return Stream.of(
                Arguments.of("decimal", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30.5]}
                        """),
                Arguments.of("string", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": ["abc"]}
                        """),
                Arguments.of("boolean", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [true]}
                        """),
                Arguments.of("object", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [{"x":1}]}
                        """),
                Arguments.of("nested array", """
                        {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [[1]]}
                        """)
        );
    }

    @ParameterizedTest(name = "ages[0] as {0} → 400 with integer type message")
    @MethodSource("agesElementTypeMismatchCases")
    void agesElement_typeMismatch(String caseName, String body) throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.status", is(400)))
                .andExpect(jsonPath("$.message", is("ages[0] must contain only integer numbers")));

        verify(createSearchPort, never()).create(any());
    }

    @Test
    void agesElement_decimalAtIndex2_showsCorrectIndex() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [10, 20, 3.5]}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message", is("ages[2] must contain only integer numbers")));

        verify(createSearchPort, never()).create(any());
    }

    @Test
    void agesElement_stringNumberAtIndex1_returns400() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [10, "30"]}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message", is("ages[1] must contain only integer numbers")));

        verify(createSearchPort, never()).create(any());
    }

    static Stream<Arguments> bodyInvalidCases() {
        return Stream.of(
                Arguments.of("empty", ""),
                Arguments.of("malformed JSON", "{invalid json"),
                Arguments.of("null literal", "null"),
                Arguments.of("array", "[1,2,3]"),
                Arguments.of("number", "123"),
                Arguments.of("string", "\"hello\""),
                Arguments.of("boolean", "true")
        );
    }

    @Test
    void valid_hotelId50Characters_returns200() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "%s", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30]}
                                """.formatted("a".repeat(50))))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.searchId", notNullValue()));
    }

    @Test
    void valid_singleAge_returns200() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [50]}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.searchId", notNullValue()));

        verify(createSearchPort).create(any());
    }

    @Test
    void valid_50Ages_returns200() throws Exception {
        var ages = IntStream.range(0, 50).mapToObj(i -> "100").collect(Collectors.joining(","));
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [%s]}
                                """.formatted(ages)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.searchId", notNullValue()));

        verify(createSearchPort).create(any());
    }

    @Test
    void valid_age0_returns200() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [0]}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.searchId", notNullValue()));
    }

    @Test
    void valid_age100_returns200() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [100]}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.searchId", notNullValue()));
    }

    @Test
    void valid_duplicateAgesPreserved_returns200() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [5, 5, 5]}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.searchId", notNullValue()));

        verify(createSearchPort).create(argThat(cmd ->
                cmd.ages().equals(List.of(5, 5, 5))));
    }

    @Test
    void valid_ageOrderPreserved_returns200() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "31/12/2030", "ages": [30, 29, 1, 3]}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.searchId", notNullValue()));

        verify(createSearchPort).create(argThat(cmd ->
                cmd.ages().equals(List.of(30, 29, 1, 3))));
    }

    @Test
    void valid_checkOutDayAfterCheckIn_returns200() throws Exception {
        mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"hotelId": "h1", "checkIn": "29/12/2030", "checkOut": "30/12/2030", "ages": [30]}
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.searchId", notNullValue()));
    }

    private static CreateSearchInput argThat(java.util.function.Predicate<CreateSearchInput> predicate) {
        return ArgumentMatchers.argThat(predicate::test);
    }
    @ParameterizedTest
    @ValueSource(strings = {"2026-10-12", "2020-01-01"})
    void pastCheckInShouldReturn400UsingInjectedClock(String date) throws Exception {
        String checkIn = java.time.LocalDate.parse(date).format(java.time.format.DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        String body = "{\"hotelId\":\"h1\",\"checkIn\":\"%s\",\"checkOut\":\"14/10/2026\",\"ages\":[30]}".formatted(checkIn);
        mockMvc.perform(post("/search").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]").value("checkIn: checkIn must not be in the past"));
        verify(createSearchPort, never()).create(any());
    }

    @Test
    void todayShouldBeAcceptedUsingInjectedClock() throws Exception {
        mockMvc.perform(post("/search").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"hotelId\":\"h1\",\"checkIn\":\"13/10/2026\",\"checkOut\":\"14/10/2026\",\"ages\":[30]}"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.searchId").isNotEmpty());
    }

    @Test
    void pastCheckOutShouldReturn400UsingInjectedClock() throws Exception {
        mockMvc.perform(post("/search").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"hotelId\":\"h1\",\"checkIn\":\"13/10/2026\",\"checkOut\":\"12/10/2026\",\"ages\":[30]}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0]").value("checkOut: checkOut must not be in the past"));
        verify(createSearchPort, never()).create(any());
    }

}
