package com.sling.hotel.infrastructure.adapter.in.rest.filter;

import com.sling.hotel.infrastructure.logging.CorrelationId;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import com.sling.hotel.support.LogCapture;
import tools.jackson.databind.json.JsonMapper;
import jakarta.servlet.http.HttpServletResponse;
import ch.qos.logback.classic.Level;

import java.nio.charset.StandardCharsets;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequestLoggingFilterTest {

    private final RequestLoggingFilter filter = new RequestLoggingFilter(JsonMapper.builder().build());

    @AfterEach
    void cleanMdc() {
        MDC.clear();
    }

    @Test
    void shouldGenerateCorrelationIdWhenNotProvided() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(CorrelationId.HEADER)).isNotBlank();
    }

    @Test
    void shouldPreserveClientCorrelationId() throws Exception {
        var request = new MockHttpServletRequest("POST", "/search");
        request.addHeader(CorrelationId.HEADER, "client-abc-123");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(CorrelationId.HEADER))
                .isEqualTo("client-abc-123");
    }

    @Test
    void shouldAcceptUuidFormatCorrelationId() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        request.addHeader(CorrelationId.HEADER, "550e8400-e29b-41d4-a716-446655440000");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(CorrelationId.HEADER))
                .isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    }

    @Test
    void shouldGenerateNewIdWhenHeaderIsBlank() throws Exception {
        var request = new MockHttpServletRequest("GET", "/count");
        request.addHeader(CorrelationId.HEADER, "   ");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        String correlationId = response.getHeader(CorrelationId.HEADER);
        assertThat(correlationId).isNotBlank().isNotEqualTo("   ");
    }

    @Test
    void shouldRejectOverlyLongCorrelationId() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        request.addHeader(CorrelationId.HEADER, "a".repeat(201));
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        String correlationId = response.getHeader(CorrelationId.HEADER);
        assertThat(correlationId).hasSizeLessThanOrEqualTo(CorrelationId.MAX_LENGTH);
    }

    @Test
    void shouldAcceptCorrelationIdAtMaxLength() throws Exception {
        String maxLengthId = "a".repeat(CorrelationId.MAX_LENGTH);
        var request = new MockHttpServletRequest("GET", "/search");
        request.addHeader(CorrelationId.HEADER, maxLengthId);
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(CorrelationId.HEADER))
                .isEqualTo(maxLengthId);
    }

    @Test
    void shouldRejectCorrelationIdWithSpaces() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        request.addHeader(CorrelationId.HEADER, "id with spaces");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        String correlationId = response.getHeader(CorrelationId.HEADER);
        assertThat(correlationId).doesNotContain(" ");
    }

    @Test
    void shouldRejectCorrelationIdWithControlCharacters() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        request.addHeader(CorrelationId.HEADER, "id\r\nX-Injected: true");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        String correlationId = response.getHeader(CorrelationId.HEADER);
        assertThat(correlationId).doesNotContain("\r").doesNotContain("\n");
    }

    @Test
    void shouldSetMdcDuringRequestProcessing() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        var response = new MockHttpServletResponse();
        var capturedMdc = new AtomicReference<String>();

        FilterChain chain = (req, res) -> capturedMdc.set(MDC.get(CorrelationId.MDC_KEY));
        filter.doFilter(request, response, chain);

        assertThat(capturedMdc.get()).isNotBlank();
    }

    @Test
    void shouldClearMdcAfterRequest() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }

    @Test
    void shouldClearMdcAndPropagateOnException() {
        var request = new MockHttpServletRequest("GET", "/search");
        var response = new MockHttpServletResponse();
        FilterChain failingChain = (req, res) -> { throw new ServletException("boom"); };

        assertAll(
                () -> assertThatThrownBy(() -> filter.doFilter(request, response, failingChain))
                .isInstanceOf(ServletException.class)
                .hasMessage("boom"),
                () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
    }

    @Test
    void shouldSetCorrelationIdInResponseEvenOnException() {
        var request = new MockHttpServletRequest("POST", "/search");
        var response = new MockHttpServletResponse();
        FilterChain failingChain = (req, res) -> { throw new ServletException("error"); };

        assertAll(
                () -> assertThatThrownBy(() -> filter.doFilter(request, response, failingChain))
                .isInstanceOf(ServletException.class),
                () -> assertThat(response.getHeader(CorrelationId.HEADER)).isNotBlank());
    }

    @Test
    void mdcCorrelationIdShouldMatchResponseHeader() throws Exception {
        var request = new MockHttpServletRequest("POST", "/search");
        var response = new MockHttpServletResponse();
        var capturedMdc = new AtomicReference<String>();

        FilterChain chain = (req, res) -> capturedMdc.set(MDC.get(CorrelationId.MDC_KEY));
        filter.doFilter(request, response, chain);

        assertThat(capturedMdc.get())
                .isEqualTo(response.getHeader(CorrelationId.HEADER));
    }

    @Test
    void shouldNotInterfereWithResponseStatus() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        var response = new MockHttpServletResponse();
        FilterChain chain = (req, res) -> ((HttpServletResponse) res).setStatus(404);

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(404);
    }

    @Test
    void debugLoggingShouldPreserveBodiesAndNeverLogHeadersOrUnknownCredentials() throws Exception {
        String body = "{\"hotelId\":\"h1\",\"checkIn\":\"29/12/2026\",\"checkOut\":\"31/12/2026\",\"ages\":[30],\"password\":\"secret-password\"}";
        var request = new MockHttpServletRequest("POST", "/search");
        request.setContentType("application/json");
        request.setCharacterEncoding("UTF-8");
        request.setContent(body.getBytes(StandardCharsets.UTF_8));
        request.addHeader("Authorization", "secret-token");
        request.addHeader("X-Correlation-ID", "http-correlation");
        var response = new MockHttpServletResponse();
        String responseJson = "{\"searchId\":\"search-1\"}";

        try (var logs = new LogCapture(RequestLoggingFilter.class.getName())) {
            filter.doFilter(request, response, (req, res) -> {
                assertThat(new String(req.getInputStream().readAllBytes(), StandardCharsets.UTF_8)).isEqualTo(body);
                res.setContentType("application/json");
                res.getWriter().write(responseJson);
            });
            assertAll(
                    () -> assertThat(response.getContentAsString()).isEqualTo(responseJson),
                    () -> assertThat(response.getHeader("X-Correlation-ID")).isEqualTo("http-correlation"),
                    () -> assertThat(logs.events()).allSatisfy(event ->
                    assertThat(event.getMDCPropertyMap()).containsEntry("correlationId", "http-correlation")),
                    () -> assertThat(logs.events().stream().map(e -> e.getFormattedMessage())).anyMatch(s -> s.contains("\"hotelId\":\"h1\""))
                    .anyMatch(s -> s.contains(responseJson))
                    .noneMatch(s -> s.contains("secret-password") || s.contains("secret-token")),
                    () -> assertThat(logs.events().stream().filter(e -> e.getLevel() == Level.INFO)).hasSize(1));
        }
        assertThat(MDC.get("correlationId")).isNull();
    }

    @Test
    void getCountShouldLogSearchIdAndResponseIncludingCount() throws Exception {
        var request = new MockHttpServletRequest("GET", "/count");
        request.addParameter("searchId", "search-1");
        var response = new MockHttpServletResponse();
        try (var logs = new LogCapture(RequestLoggingFilter.class.getName())) {
            filter.doFilter(request, response, (req, res) -> {
                ((HttpServletResponse) res).setStatus(200);
                res.getWriter().write("{\"searchId\":\"search-1\",\"search\":{},\"count\":2}");
            });
            assertAll(
                    () -> assertThat(response.getContentAsString()).contains("\"count\":2"),
                    () -> assertThat(logs.events().stream().map(e -> e.getFormattedMessage()))
                    .anyMatch(s -> s.contains("searchId=search-1"))
                    .anyMatch(s -> s.contains("\"count\":2"))
                    .anyMatch(s -> s.contains("GET /count 200") && s.endsWith("ms")));
        }
        assertThat(MDC.get("correlationId")).isNull();
    }

    @Test
    void debugLoggingShouldCopyResponseAndClearMdcEvenWhenChainThrows() {
        var request = new MockHttpServletRequest("POST", "/search");
        var response = new MockHttpServletResponse();
        try (var logs = new LogCapture(RequestLoggingFilter.class.getName())) {
            assertAll(
                    () -> assertThatThrownBy(() -> filter.doFilter(request, response, (req, res) -> {
                res.getWriter().write("{\"message\":\"partial\"}");
                throw new ServletException("failure");
            })).isInstanceOf(ServletException.class),
                    () -> assertThat(response.getContentAsString()).contains("partial"),
                    () -> assertThat(logs.events().stream().filter(e -> e.getLevel() == Level.INFO)).hasSize(1));
        }
        assertThat(MDC.get("correlationId")).isNull();
    }

    @Test
    void malformedBodyShouldLogSanitizationFailureWithoutExposingContent() throws Exception {
        var request = new MockHttpServletRequest("POST", "/search");
        request.setContent("{password=private-secret".getBytes(StandardCharsets.UTF_8));
        var response = new MockHttpServletResponse();
        try (var logs = new LogCapture(RequestLoggingFilter.class.getName())) {
            filter.doFilter(request, response, (req, res) -> {
                req.getInputStream().readAllBytes();
                ((HttpServletResponse) res).setStatus(400);
                res.getWriter().write("{\"status\":400}");
            });
            assertAll(
                    () -> assertThat(logs.events()).anySatisfy(event ->
                    assertThat(event.getFormattedMessage()).contains("HTTP body omitted from logs: exceptionType=")),
                    () -> assertThat(logs.events()).noneSatisfy(event ->
                    assertThat(event.getFormattedMessage()).contains("private-secret")),
                    () -> assertThat(response.getStatus()).isEqualTo(400));
        }
    }

    @Test
    void infoLoggingShouldLeaveBodiesUntouchedAndSkipBodyLogs() throws Exception {
        var request = new MockHttpServletRequest("POST", "/search");
        String body = "{\"hotelId\":\"h1\"}";
        request.setContent(body.getBytes(StandardCharsets.UTF_8));
        var response = new MockHttpServletResponse();
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(RequestLoggingFilter.class);
        try (var logs = new LogCapture(RequestLoggingFilter.class.getName())) {
            logger.setLevel(Level.INFO);
            filter.doFilter(request, response, (req, res) -> {
                assertThat(new String(req.getInputStream().readAllBytes(), StandardCharsets.UTF_8)).isEqualTo(body);
                ((HttpServletResponse) res).setStatus(202);
                res.getWriter().write("{\"searchId\":\"search-1\"}");
            });
            assertAll(
                    () -> assertThat(response.getStatus()).isEqualTo(202),
                    () -> assertThat(response.getContentAsString()).isEqualTo("{\"searchId\":\"search-1\"}"),
                    () -> assertThat(logs.events()).singleElement().satisfies(event ->
                            assertThat(event.getFormattedMessage()).contains("POST /search 202")),
                    () -> assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull());
        }
    }

}
