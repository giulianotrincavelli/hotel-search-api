package com.sling.hotel.infrastructure.adapter.in.rest.filter;

import com.sling.hotel.infrastructure.logging.CorrelationId;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class RequestLoggingFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);
    static final String CORRELATION_HEADER = CorrelationId.HEADER;
    static final String MDC_CORRELATION_ID = CorrelationId.MDC_KEY;
    static final int MAX_CORRELATION_ID_LENGTH = CorrelationId.MAX_LENGTH;
    private static final int BODY_LOG_LIMIT = 16 * 1024;
    private final JsonMapper jsonMapper;

    public RequestLoggingFilter(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String correlationId = resolveCorrelationId(request);
        MDC.put(MDC_CORRELATION_ID, correlationId);
        response.setHeader(CORRELATION_HEADER, correlationId);

        boolean logBodies = log.isDebugEnabled() && (
                ("POST".equals(request.getMethod()) && (request.getContextPath() + "/search").equals(request.getRequestURI()))
                || ("GET".equals(request.getMethod()) && (request.getContextPath() + "/count").equals(request.getRequestURI())));
        var cachedRequest = logBodies ? new ContentCachingRequestWrapper(request, BODY_LOG_LIMIT) : null;
        var cachedResponse = logBodies ? new ContentCachingResponseWrapper(response) : null;
        long startNanos = System.nanoTime();
        boolean failed = false;
        try {
            chain.doFilter(logBodies ? cachedRequest : request, logBodies ? cachedResponse : response);
        } catch (Exception e) {
            failed = true;
            log.error("HTTP processing failed: method={} path={}", request.getMethod(), request.getRequestURI(), e);
            throw e;
        } finally {
            try {
                if (logBodies) {
                    if ("POST".equals(request.getMethod())) {
                        log.debug("HTTP request: method=POST path=/search body={}",
                                jsonForLog(cachedRequest.getContentAsByteArray(), request.getCharacterEncoding(), true));
                    } else {
                        log.debug("HTTP request: method=GET path=/count searchId={}",
                                singleLine(request.getParameter("searchId")));
                    }
                    log.debug("HTTP response: method={} path={} status={} body={}", request.getMethod(),
                            request.getRequestURI(), cachedResponse.getStatus(),
                            jsonForLog(cachedResponse.getContentAsByteArray(), cachedResponse.getCharacterEncoding(), false));
                }
                log.info("{} {} {} {}ms", request.getMethod(), request.getRequestURI(),
                        failed ? 500 : response.getStatus(), durationMs(startNanos));
            } finally {
                try {
                    if (cachedResponse != null) {
                        cachedResponse.copyBodyToResponse();
                    }
                } finally {
                    MDC.remove(MDC_CORRELATION_ID);
                }
            }
        }
    }

    static String resolveCorrelationId(HttpServletRequest request) {
        return CorrelationId.resolve(request.getHeader(CORRELATION_HEADER));
    }

    private String jsonForLog(byte[] body, String encoding, boolean request) {
        if (body.length == 0) {
            return "<empty>";
        }
        if (body.length >= BODY_LOG_LIMIT) {
            return "<body exceeds logging limit>";
        }
        try {
            var json = jsonMapper.readTree(new String(body,
                    encoding == null ? StandardCharsets.UTF_8 : Charset.forName(encoding)));
            if (request) {
                // Log only contract fields, never unexpected credential fields in invalid requests.
                var fields = jsonMapper.createObjectNode();
                for (String name : List.of("hotelId", "checkIn", "checkOut", "ages")) {
                    if (json.has(name) && isSafeValue(json.get(name))) {
                        fields.set(name, json.get(name));
                    }
                }
                json = fields;
            }
            return jsonMapper.writeValueAsString(json);
        } catch (Exception e) {
            return "<unreadable JSON>";
        }
    }

    private static boolean isSafeValue(JsonNode node) {
        if (!node.isContainer()) {
            return true;
        }
        if (node.isArray()) {
            for (var element : node) {
                if (element.isContainer()) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static String singleLine(String value) {
        return value == null ? null : value.replace("\r", "\\r").replace("\n", "\\n");
    }

    private static long durationMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }
}
