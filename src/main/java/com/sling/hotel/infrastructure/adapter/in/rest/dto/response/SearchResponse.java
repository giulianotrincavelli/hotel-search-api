package com.sling.hotel.infrastructure.adapter.in.rest.dto.response;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Response after registering a hotel search")
public record SearchResponse(

        @Schema(description = "Unique search identifier", example = "550e8400-e29b-41d4-a716-446655440000")
        String searchId
) {
}
