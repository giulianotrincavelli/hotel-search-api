package com.sling.hotel.infrastructure.adapter.in.rest.dto.response;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

@Schema(description = "Error response")
public record ErrorResponse(

        @Schema(description = "HTTP status code", example = "400")
        int status,

        @Schema(description = "Error message", example = "Validation failed")
        String message,

        @Schema(description = "Detailed error descriptions", nullable = true)
        List<String> errors
) {

    public ErrorResponse {
        errors = errors == null ? null : List.copyOf(errors);
    }

    public ErrorResponse(int status, String message) {
        this(status, message, null);
    }
}
