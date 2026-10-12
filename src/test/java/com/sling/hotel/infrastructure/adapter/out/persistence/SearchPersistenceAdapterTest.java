package com.sling.hotel.infrastructure.adapter.out.persistence;

import com.sling.hotel.domain.exception.IdempotencyConflictException;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.adapter.out.persistence.entity.SearchEntity;
import com.sling.hotel.infrastructure.adapter.out.persistence.repository.SearchJpaRepository;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SearchPersistenceAdapterTest {

    @Mock
    private SearchJpaRepository jpaRepository;

    @InjectMocks
    private SearchPersistenceAdapter adapter;

    private static final HotelSearch SEARCH = new HotelSearch(
            "search-1", "hotel-1",
            LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
            List.of(30, 29, 1, 3)
    );

    private static final SearchEntity SEARCH_ENTITY = new SearchEntity(
            "search-1", "hotel-1",
            LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
            "30,29,1,3"
    );

    @Test
    void newSearchIdShouldInsert() {
        when(jpaRepository.mergeSearch("search-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), "30,29,1,3"))
                .thenReturn(1);

        assertDoesNotThrow(() -> adapter.save(SEARCH));

        verify(jpaRepository).mergeSearch("search-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), "30,29,1,3");
        verify(jpaRepository, never()).findById(any());
    }

    @Test
    void shouldNotCallExistsByIdOnSave() {
        when(jpaRepository.mergeSearch(any(), any(), any(), any(), any())).thenReturn(1);

        adapter.save(SEARCH);

        verify(jpaRepository, never()).existsById(any());
    }

    @Test
    void sameSearchIdSamePayloadShouldBeIdempotent() {
        when(jpaRepository.mergeSearch(any(), any(), any(), any(), any())).thenReturn(0);
        when(jpaRepository.findById("search-1")).thenReturn(Optional.of(SEARCH_ENTITY));

        assertDoesNotThrow(() -> adapter.save(SEARCH));
    }

    @Test
    void sameSearchIdDifferentPayloadShouldThrowConflict() {
        when(jpaRepository.mergeSearch(any(), any(), any(), any(), any())).thenReturn(0);

        var differentEntity = new SearchEntity(
                "search-1", "hotel-DIFFERENT",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                "30,29,1,3"
        );
        when(jpaRepository.findById("search-1")).thenReturn(Optional.of(differentEntity));

        assertThatThrownBy(() -> adapter.save(SEARCH))
                .isInstanceOf(IdempotencyConflictException.class)
                .hasMessageContaining("search-1");
    }

    @Test
    void sameSearchIdDifferentAgesShouldThrowConflict() {
        when(jpaRepository.mergeSearch(any(), any(), any(), any(), any())).thenReturn(0);

        var differentAgesEntity = new SearchEntity(
                "search-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                "1,2,3,4"
        );
        when(jpaRepository.findById("search-1")).thenReturn(Optional.of(differentAgesEntity));

        assertThatThrownBy(() -> adapter.save(SEARCH))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    void concurrentPkDuplicateWithSamePayloadShouldBeIdempotent() {
        var oraError = new SQLIntegrityConstraintViolationException(
                "ORA-00001: unique constraint (TESTUSER.PK_HOTEL_SEARCH) violated", "23000", 1);
        when(jpaRepository.mergeSearch(any(), any(), any(), any(), any()))
                .thenThrow(new DataIntegrityViolationException("dup", oraError));
        when(jpaRepository.findById("search-1")).thenReturn(Optional.of(SEARCH_ENTITY));

        assertDoesNotThrow(() -> adapter.save(SEARCH));
    }

    @Test
    void concurrentPkDuplicateWithDifferentPayloadShouldThrowConflict() {
        var oraError = new SQLIntegrityConstraintViolationException(
                "ORA-00001: unique constraint (TESTUSER.PK_HOTEL_SEARCH) violated", "23000", 1);
        when(jpaRepository.mergeSearch(any(), any(), any(), any(), any()))
                .thenThrow(new DataIntegrityViolationException("dup", oraError));

        var differentEntity = new SearchEntity(
                "search-1", "hotel-DIFFERENT",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                "30,29,1,3"
        );
        when(jpaRepository.findById("search-1")).thenReturn(Optional.of(differentEntity));

        assertThatThrownBy(() -> adapter.save(SEARCH))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    void nonPkIntegrityErrorShouldPropagate() {
        var oraError = new SQLIntegrityConstraintViolationException("ORA-02291", "23000", 2291);
        when(jpaRepository.mergeSearch(any(), any(), any(), any(), any()))
                .thenThrow(new DataIntegrityViolationException("fk violation", oraError));

        assertThatThrownBy(() -> adapter.save(SEARCH))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void nonPkConstraintOra00001ShouldPropagate() {
        var oraError = new SQLIntegrityConstraintViolationException(
                "ORA-00001: unique constraint (TESTUSER.OTHER_CONSTRAINT) violated", "23000", 1);
        when(jpaRepository.mergeSearch(any(), any(), any(), any(), any()))
                .thenThrow(new DataIntegrityViolationException("other unique", oraError));

        assertThatThrownBy(() -> adapter.save(SEARCH))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void shouldFindByIdAndMapToDomain() {
        when(jpaRepository.findById("search-1")).thenReturn(Optional.of(SEARCH_ENTITY));

        Optional<HotelSearch> result = adapter.findById("search-1");

        assertThat(result).isPresent();
        HotelSearch found = result.get();
        assertAll(
                () -> assertThat(found.searchId()).isEqualTo("search-1"),
                () -> assertThat(found.hotelId()).isEqualTo("hotel-1"),
                () -> assertThat(found.ages()).containsExactly(30, 29, 1, 3)
        );
    }

    @Test
    void shouldReturnEmptyWhenNotFound() {
        when(jpaRepository.findById("missing")).thenReturn(Optional.empty());

        assertThat(adapter.findById("missing")).isEmpty();
    }

    @Test
    void shouldCountIdenticalSearches() {
        when(jpaRepository.countByIdentity("hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                "30,29,1,3"))
                .thenReturn(5L);

        assertThat(adapter.countIdentical(SEARCH)).isEqualTo(5L);
    }

    @Test
    void shouldCountZeroWhenNoIdenticalSearches() {
        when(jpaRepository.countByIdentity("hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                "30,29,1,3"))
                .thenReturn(0L);

        assertThat(adapter.countIdentical(SEARCH)).isEqualTo(0L);
    }

    @Test
    void shouldDistinguishDifferentAgeOrders() {
        var search1 = new HotelSearch(
                "s1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                List.of(30, 29, 1, 3)
        );
        var search2 = new HotelSearch(
                "s2", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                List.of(3, 29, 30, 1)
        );

        when(jpaRepository.countByIdentity("hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                "30,29,1,3")).thenReturn(3L);
        when(jpaRepository.countByIdentity("hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                "3,29,30,1")).thenReturn(1L);

        assertAll(
                () -> assertThat(adapter.countIdentical(search1)).isEqualTo(3L),
                () -> assertThat(adapter.countIdentical(search2)).isEqualTo(1L)
        );
    }
    @Test
    void nestedSqlExceptionShouldBeDetectedEvenWhenItsOwnCauseIsMoreSpecific() {
        var sql = new SQLException("ORA-00001", "23000", 1,
                new RuntimeException("Oracle driver detail"));
        var constraint = new ConstraintViolationException("duplicate", sql, "\"HOTEL\".\"PK_HOTEL_SEARCH\"");
        var failure = new DataIntegrityViolationException("merge failed", new RuntimeException(constraint));
        when(jpaRepository.mergeSearch(any(), any(), any(), any(), any())).thenThrow(failure);
        when(jpaRepository.findById(SEARCH.searchId())).thenReturn(Optional.of(SEARCH_ENTITY));
        assertDoesNotThrow(() -> adapter.save(SEARCH));
        verify(jpaRepository).findById(SEARCH.searchId());
    }

    @Test
    void reportedConstraintNameShouldTakePrecedenceOverSqlMessage() {
        var sql = new SQLException("ORA-00001 (HOTEL.PK_HOTEL_SEARCH)", "23000", 1);
        var constraint = new ConstraintViolationException("duplicate", sql, "OTHER_CONSTRAINT");
        var failure = new DataIntegrityViolationException("merge failed", constraint);
        when(jpaRepository.mergeSearch(any(), any(), any(), any(), any())).thenThrow(failure);
        assertThatThrownBy(() -> adapter.save(SEARCH)).isSameAs(failure);
        verify(jpaRepository, never()).findById(any());
    }

    @Test
    void absentWinnerAfterPkDuplicateShouldReportConflict() {
        var sql = new SQLException("ORA-00001 (HOTEL.PK_HOTEL_SEARCH)", "23000", 1);
        var failure = new DataIntegrityViolationException("merge failed", sql);
        when(jpaRepository.mergeSearch(any(), any(), any(), any(), any())).thenThrow(failure);
        when(jpaRepository.findById(SEARCH.searchId())).thenReturn(Optional.empty());
        assertThatThrownBy(() -> adapter.save(SEARCH)).isInstanceOf(IdempotencyConflictException.class);
    }
}
