package com.sling.hotel.infrastructure.logging;

import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Pattern;

public final class CorrelationId {

    public static final String HEADER = "X-Correlation-ID";
    public static final String MDC_KEY = "correlationId";
    public static final int MAX_LENGTH = 200;
    private static final Pattern VALID_VALUE = Pattern.compile("[a-zA-Z0-9._:-]+");

    private CorrelationId() {
    }

    public static String resolve(String value) {
        return value != null && value.length() <= MAX_LENGTH && VALID_VALUE.matcher(value).matches()
                ? value : UUID.randomUUID().toString();
    }

    public static String fromHeaders(Headers headers) {
        var header = headers.lastHeader(HEADER);
        String value = header == null || header.value() == null ? null
                : new String(header.value(), StandardCharsets.UTF_8);
        String resolved = resolve(value);
        if (!resolved.equals(value)) {
            headers.remove(HEADER);
            headers.add(HEADER, resolved.getBytes(StandardCharsets.UTF_8));
        }
        return resolved;
    }

    public static void withHeaders(Headers headers, Runnable action) {
        String previous = MDC.get(MDC_KEY);
        try {
            MDC.put(MDC_KEY, fromHeaders(headers));
            action.run();
        } finally {
            MDC.remove(MDC_KEY);
            if (previous != null) {
                MDC.put(MDC_KEY, previous);
            }
        }
    }
}
