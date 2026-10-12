package com.sling.hotel.infrastructure.adapter.out.persistence.repository;

import com.sling.hotel.infrastructure.adapter.out.persistence.entity.SearchEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;

public interface SearchJpaRepository extends JpaRepository<SearchEntity, String> {

    @Query("SELECT COUNT(s) FROM SearchEntity s WHERE s.hotelId = :hotelId "
            + "AND s.checkIn = :checkIn AND s.checkOut = :checkOut AND s.ages = :ages")
    long countByIdentity(String hotelId, LocalDate checkIn, LocalDate checkOut, String ages);

    @Modifying
    @Transactional
    @Query(value = "MERGE INTO hotel_search t "
            + "USING (SELECT :searchId AS search_id FROM DUAL) s "
            + "ON (t.search_id = s.search_id) "
            + "WHEN NOT MATCHED THEN INSERT (search_id, hotel_id, check_in, check_out, ages) "
            + "VALUES (:searchId, :hotelId, :checkIn, :checkOut, :ages)",
            nativeQuery = true)
    int mergeSearch(String searchId, String hotelId, LocalDate checkIn, LocalDate checkOut, String ages);
}
