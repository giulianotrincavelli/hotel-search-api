package com.sling.hotel.infrastructure.adapter.in.rest.filter;

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

        assertThat(response.getHeader(RequestLoggingFilter.CORRELATION_HEADER)).isNotBlank();
    }

    @Test
    void shouldPreserveClientCorrelationId() throws Exception {
        var request = new MockHttpServletRequest("POST", "/search");
        request.addHeader(RequestLoggingFilter.CORRELATION_HEADER, "client-abc-123");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(RequestLoggingFilter.CORRELATION_HEADER))
                .isEqualTo("client-abc-123");
    }

    @Test
    void shouldAcceptUuidFormatCorrelationId() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        request.addHeader(RequestLoggingFilter.CORRELATION_HEADER, "550e8400-e29b-41d4-a716-446655440000");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(RequestLoggingFilter.CORRELATION_HEADER))
                .isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    }

    @Test
    void shouldGenerateNewIdWhenHeaderIsBlank() throws Exception {
        var request = new MockHttpServletRequest("GET", "/count");
        request.addHeader(RequestLoggingFilter.CORRELATION_HEADER, "   ");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        String correlationId = response.getHeader(RequestLoggingFilter.CORRELATION_HEADER);
        assertThat(correlationId).isNotBlank().isNotEqualTo("   ");
    }

    @Test
    void shouldRejectOverlyLongCorrelationId() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        request.addHeader(RequestLoggingFilter.CORRELATION_HEADER, "a".repeat(201));
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        String correlationId = response.getHeader(RequestLoggingFilter.CORRELATION_HEADER);
        assertThat(correlationId).hasSizeLessThanOrEqualTo(RequestLoggingFilter.MAX_CORRELATION_ID_LENGTH);
    }

    @Test
    void shouldAcceptCorrelationIdAtMaxLength() throws Exception {
        String maxLengthId = "a".repeat(RequestLoggingFilter.MAX_CORRELATION_ID_LENGTH);
        var request = new MockHttpServletRequest("GET", "/search");
        request.addHeader(RequestLoggingFilter.CORRELATION_HEADER, maxLengthId);
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(RequestLoggingFilter.CORRELATION_HEADER))
                .isEqualTo(maxLengthId);
    }

    @Test
    void shouldRejectCorrelationIdWithSpaces() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        request.addHeader(RequestLoggingFilter.CORRELATION_HEADER, "id with spaces");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        String correlationId = response.getHeader(RequestLoggingFilter.CORRELATION_HEADER);
        assertThat(correlationId).doesNotContain(" ");
    }

    @Test
    void shouldRejectCorrelationIdWithControlCharacters() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        request.addHeader(RequestLoggingFilter.CORRELATION_HEADER, "id\r\nX-Injected: true");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        String correlationId = response.getHeader(RequestLoggingFilter.CORRELATION_HEADER);
        assertThat(correlationId).doesNotContain("\r").doesNotContain("\n");
    }

    @Test
    void shouldSetMdcDuringRequestProcessing() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        var response = new MockHttpServletResponse();
        var capturedMdc = new AtomicReference<String>();

        FilterChain chain = (req, res) -> capturedMdc.set(MDC.get(RequestLoggingFilter.MDC_CORRELATION_ID));
        filter.doFilter(request, response, chain);

        assertThat(capturedMdc.get()).isNotBlank();
    }

    @Test
    void shouldClearMdcAfterRequest() throws Exception {
        var request = new MockHttpServletRequest("GET", "/search");
        var response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(MDC.get(RequestLoggingFilter.MDC_CORRELATION_ID)).isNull();
    }

    @Test
    void shouldClearMdcAndPropagateOnException() {
        var request = new MockHttpServletRequest("GET", "/search");
        var response = new MockHttpServletResponse();
        FilterChain failingChain = (req, res) -> { throw new ServletException("boom"); };

        assertThatThrownBy(() -> filter.doFilter(request, response, failingChain))
                .isInstanceOf(ServletException.class)
                .hasMessage("boom");

        assertThat(MDC.get(RequestLoggingFilter.MDC_CORRELATION_ID)).isNull();
    }

    @Test
    void shouldSetCorrelationIdInResponseEvenOnException() {
        var request = new MockHttpServletRequest("POST", "/search");
        var response = new MockHttpServletResponse();
        FilterChain failingChain = (req, res) -> { throw new ServletException("error"); };

        assertThatThrownBy(() -> filter.doFilter(request, response, failingChain))
                .isInstanceOf(ServletException.class);

        assertThat(response.getHeader(RequestLoggingFilter.CORRELATION_HEADER)).isNotBlank();
    }

    @Test
    void mdcCorrelationIdShouldMatchResponseHeader() throws Exception {
        var request = new MockHttpServletRequest("POST", "/search");
        var response = new MockHttpServletResponse();
        var capturedMdc = new AtomicReference<String>();

        FilterChain chain = (req, res) -> capturedMdc.set(MDC.get(RequestLoggingFilter.MDC_CORRELATION_ID));
        filter.doFilter(request, response, chain);

        assertThat(capturedMdc.get())
                .isEqualTo(response.getHeader(RequestLoggingFilter.CORRELATION_HEADER));
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
            assertThat(response.getContentAsString()).isEqualTo(responseJson);
            assertThat(response.getHeader("X-Correlation-ID")).isEqualTo("http-correlation");
            assertThat(logs.events()).allSatisfy(event ->
                    assertThat(event.getMDCPropertyMap()).containsEntry("correlationId", "http-correlation"));
            assertThat(logs.events().stream().map(e -> e.getFormattedMessage())).anyMatch(s -> s.contains("\"hotelId\":\"h1\""))
                    .anyMatch(s -> s.contains(responseJson))
                    .noneMatch(s -> s.contains("secret-password") || s.contains("secret-token"));
            assertThat(logs.events().stream().filter(e -> e.getLevel() == Level.INFO)).hasSize(1);
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
            assertThat(response.getContentAsString()).contains("\"count\":2");
            assertThat(logs.events().stream().map(e -> e.getFormattedMessage()))
                    .anyMatch(s -> s.contains("searchId=search-1"))
                    .anyMatch(s -> s.contains("\"count\":2"))
                    .anyMatch(s -> s.contains("GET /count 200") && s.endsWith("ms"));
        }
        assertThat(MDC.get("correlationId")).isNull();
    }

    @Test
    void debugLoggingShouldCopyResponseAndClearMdcEvenWhenChainThrows() {
        var request = new MockHttpServletRequest("POST", "/search");
        var response = new MockHttpServletResponse();
        try (var logs = new LogCapture(RequestLoggingFilter.class.getName())) {
            assertThatThrownBy(() -> filter.doFilter(request, response, (req, res) -> {
                res.getWriter().write("{\"message\":\"partial\"}");
                throw new ServletException("failure");
            })).isInstanceOf(ServletException.class);
            assertThat(response.getContentAsString()).contains("partial");
            assertThat(logs.events().stream().filter(e -> e.getLevel() == Level.INFO)).hasSize(1);
        } catch (java.io.UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
        assertThat(MDC.get("correlationId")).isNull();
    }
}
