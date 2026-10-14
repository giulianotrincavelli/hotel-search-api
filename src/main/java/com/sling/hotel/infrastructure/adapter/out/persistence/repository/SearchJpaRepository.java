package com.sling.hotel.infrastructure.adapter.out.persistence.repository;

import com.sling.hotel.infrastructure.adapter.out.persistence.entity.SearchEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.Optional;

public interface SearchJpaRepository extends JpaRepository<SearchEntity, String> {

    @Query("SELECT new com.sling.hotel.infrastructure.adapter.out.persistence.repository.SearchCountRow("
            + "s.searchId, s.hotelId, s.checkIn, s.checkOut, s.ages, "
            + "(SELECT COUNT(*) FROM SearchEntity identical WHERE identical.hotelId = s.hotelId "
            + "AND identical.checkIn = s.checkIn AND identical.checkOut = s.checkOut "
            + "AND identical.ages = s.ages)) FROM SearchEntity s WHERE s.searchId = :searchId")
    Optional<SearchCountRow> findWithCount(String searchId);

    @Modifying
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Query(value = "INSERT INTO hotel_search (search_id, hotel_id, check_in, check_out, ages) "
            + "VALUES (:searchId, :hotelId, :checkIn, :checkOut, :ages)", nativeQuery = true)
    int insertSearch(String searchId, String hotelId, LocalDate checkIn, LocalDate checkOut, String ages);
}
