package com.sling.hotel.infrastructure.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.exc.InvalidFormatException;
import tools.jackson.databind.exc.MismatchedInputException;
import tools.jackson.databind.json.JsonMapper;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JacksonConfigTest {

    private JsonMapper mapper;

    @BeforeEach
    void setUp() {
        var builder = JsonMapper.builder();
        var config = new JacksonConfig();
        config.strictLocalDateCustomizer().customize(builder);
        config.strictIntegerTypesCustomizer().customize(builder);
        config.strictStringTypesCustomizer().customize(builder);
        config.strictLocalDateNoTimestampsCustomizer().customize(builder);
        mapper = builder.build();
    }

    @Test
    void shouldParseValidDate() throws Exception {
        LocalDate date = mapper.readValue("\"29/12/2023\"", LocalDate.class);
        assertThat(date).isEqualTo(LocalDate.of(2023, 12, 29));
    }

    @Test
    void shouldSerializeDateInDdMmYyyyFormat() throws Exception {
        String json = mapper.writeValueAsString(LocalDate.of(2023, 12, 29));
        assertThat(json).isEqualTo("\"29/12/2023\"");
    }

    @Test
    void shouldRejectInvalidDate31Feb() {
        assertThatThrownBy(() -> mapper.readValue("\"31/02/2023\"", LocalDate.class))
                .isInstanceOf(InvalidFormatException.class);
    }

    @Test
    void shouldRejectIsoFormatDate() {
        assertThatThrownBy(() -> mapper.readValue("\"2023-12-29\"", LocalDate.class))
                .isInstanceOf(InvalidFormatException.class);
    }

    @Test
    void shouldRejectFloatAsInt() {
        assertThatThrownBy(() -> mapper.readValue("30.5", Integer.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    @Test
    void shouldRejectStringAsInt() {
        assertThatThrownBy(() -> mapper.readValue("\"30\"", Integer.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    @Test
    void shouldRejectBooleanAsInt() {
        assertThatThrownBy(() -> mapper.readValue("true", Integer.class))
                .isInstanceOf(MismatchedInputException.class);
    }

    @Test
    void shouldAcceptValidInteger() throws Exception {
        Integer value = mapper.readValue("30", Integer.class);
        assertThat(value).isEqualTo(30);
    }
}
