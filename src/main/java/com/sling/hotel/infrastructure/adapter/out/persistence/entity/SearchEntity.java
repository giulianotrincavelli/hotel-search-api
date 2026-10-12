package com.sling.hotel.infrastructure.adapter.out.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.LocalDate;

@Entity
@Table(name = "hotel_search")
public class SearchEntity {

    @Id
    @Column(name = "search_id", length = 36, nullable = false)
    private String searchId;

    @Column(name = "hotel_id", length = 50, nullable = false)
    private String hotelId;

    @Column(name = "check_in", nullable = false)
    private LocalDate checkIn;

    @Column(name = "check_out", nullable = false)
    private LocalDate checkOut;

    // Comma-separated ages preserving order ("30,29,1,3") for deterministic counting.
    @Column(name = "ages", length = 200, nullable = false)
    private String ages;

    protected SearchEntity() {
    }

    public SearchEntity(String searchId, String hotelId, LocalDate checkIn, LocalDate checkOut, String ages) {
        this.searchId = searchId;
        this.hotelId = hotelId;
        this.checkIn = checkIn;
        this.checkOut = checkOut;
        this.ages = ages;
    }

    public String getSearchId() {
        return searchId;
    }

    public String getHotelId() {
        return hotelId;
    }

    public LocalDate getCheckIn() {
        return checkIn;
    }

    public LocalDate getCheckOut() {
        return checkOut;
    }

    public String getAges() {
        return ages;
    }
}
