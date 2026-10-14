package com.sling.hotel.infrastructure.adapter.in.rest.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.FutureOrPresent;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Schema(description = "Hotel availability search request")
public record SearchRequest(

        @NotBlank(message = "hotelId is required")
        @Size(max = 50, message = "hotelId must have at most 50 characters")
        @Schema(description = "Hotel identifier", example = "1234aBc")
        String hotelId,

        @NotNull(message = "checkIn is required")
        @FutureOrPresent(message = "checkIn must not be in the past")
        @Schema(description = "Check-in date in dd/MM/yyyy format", example = "29/12/2026")
        LocalDate checkIn,

        @NotNull(message = "checkOut is required")
        @FutureOrPresent(message = "checkOut must not be in the past")
        @Schema(description = "Check-out date in dd/MM/yyyy format", example = "31/12/2026")
        LocalDate checkOut,

        @NotEmpty(message = "ages is required and must not be empty")
        @Size(max = 50, message = "ages must have at most 50 elements")
        @Schema(description = "List of guest ages, each 0-100 inclusive (order matters)", example = "[30, 29, 1, 3]")
        List<@NotNull(message = "each age must not be null")
             @Min(value = 0, message = "each age must be an integer between 0 and 100")
             @Max(value = 100, message = "each age must be an integer between 0 and 100") Integer> ages
) {

    public SearchRequest {
        ages = ages == null ? null : Collections.unmodifiableList(new ArrayList<>(ages));
    }
}
