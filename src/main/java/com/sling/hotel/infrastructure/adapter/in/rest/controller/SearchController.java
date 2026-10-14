package com.sling.hotel.infrastructure.adapter.in.rest.controller;

import com.sling.hotel.application.ports.in.CountSearchPort;
import com.sling.hotel.application.ports.in.CreateSearchPort;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.adapter.in.rest.dto.request.SearchRequest;
import com.sling.hotel.infrastructure.adapter.in.rest.dto.response.CountResponse;
import com.sling.hotel.infrastructure.adapter.in.rest.dto.response.ErrorResponse;
import com.sling.hotel.infrastructure.adapter.in.rest.dto.response.SearchResponse;
import com.sling.hotel.infrastructure.adapter.in.rest.mapper.SearchRequestMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@Tag(name = "Hotel Search", description = "Hotel availability search registration and counting")
public class SearchController {

    private final CreateSearchPort createSearchPort;
    private final CountSearchPort countSearchPort;

    public SearchController(CreateSearchPort createSearchPort, CountSearchPort countSearchPort) {
        this.createSearchPort = createSearchPort;
        this.countSearchPort = countSearchPort;
    }

    @PostMapping("/search")
    @Operation(
            summary = "Register a hotel availability search",
            description = "Validates the search, generates a unique searchId, and publishes it to Kafka for asynchronous persistence. "
                    + "The search may not be immediately available via GET /count due to asynchronous processing.",
            responses = {
                    @ApiResponse(responseCode = "202", description = "Search accepted for asynchronous persistence"),
                    @ApiResponse(responseCode = "400", description = "Invalid request",
                            content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
                    @ApiResponse(responseCode = "503", description = "Kafka unavailable",
                            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
            }
    )
    public ResponseEntity<SearchResponse> createSearch(@Valid @RequestBody SearchRequest request) {
        HotelSearch created = createSearchPort.create(SearchRequestMapper.toInput(request));
        return ResponseEntity.accepted().body(new SearchResponse(created.searchId()));
    }

    @GetMapping("/count")
    @Operation(
            summary = "Count identical hotel searches",
            description = "Returns the search details and the count of identical searches persisted in Oracle. "
                    + "Since persistence is asynchronous, a recently created search may not be immediately available.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Search found and count returned"),
                    @ApiResponse(responseCode = "400", description = "Missing or blank searchId",
                            content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
                    @ApiResponse(responseCode = "404", description = "Search not found",
                            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
            }
    )
    public ResponseEntity<CountResponse> countSearch(
            @RequestParam @NotBlank(message = "searchId is required") String searchId) {
        return ResponseEntity.ok(SearchRequestMapper.toCountResponse(countSearchPort.count(searchId)));
    }
}
