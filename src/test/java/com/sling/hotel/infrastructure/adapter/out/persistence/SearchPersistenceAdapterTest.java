package com.sling.hotel.infrastructure.adapter.out.persistence;

import com.sling.hotel.domain.model.HotelSearch;
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

    @Test
    void newSearchIdShouldInsert() {
        when(jpaRepository.insertSearch("search-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), "30,29,1,3"))
                .thenReturn(1);

        assertAll(
                () -> assertDoesNotThrow(() -> adapter.save(SEARCH)),
                () -> verify(jpaRepository).insertSearch("search-1", "hotel-1",
                        LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), "30,29,1,3"),
                () -> verify(jpaRepository, never()).findById(any()));
    }

    @Test
    void shouldNotCallExistsByIdOnSave() {
        when(jpaRepository.insertSearch(any(), any(), any(), any(), any())).thenReturn(1);

        adapter.save(SEARCH);

        verify(jpaRepository, never()).existsById(any());
    }

    @Test
    void concurrentPkDuplicateWithSamePayloadShouldBeIdempotent() {
        var oraError = new SQLIntegrityConstraintViolationException(
                "ORA-00001: unique constraint (TESTUSER.PK_HOTEL_SEARCH) violated", "23000", 1);
        when(jpaRepository.insertSearch(any(), any(), any(), any(), any()))
                .thenThrow(new DataIntegrityViolationException("dup", oraError));
        assertAll(
                () -> assertDoesNotThrow(() -> adapter.save(SEARCH)),
                () -> verify(jpaRepository, never()).findById(any()));
    }

    @Test
    void nonPkIntegrityErrorShouldPropagate() {
        var oraError = new SQLIntegrityConstraintViolationException("ORA-02291", "23000", 2291);
        when(jpaRepository.insertSearch(any(), any(), any(), any(), any()))
                .thenThrow(new DataIntegrityViolationException("fk violation", oraError));

        assertThatThrownBy(() -> adapter.save(SEARCH))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void nonPkConstraintOra00001ShouldPropagate() {
        var oraError = new SQLIntegrityConstraintViolationException(
                "ORA-00001: unique constraint (TESTUSER.OTHER_CONSTRAINT) violated", "23000", 1);
        when(jpaRepository.insertSearch(any(), any(), any(), any(), any()))
                .thenThrow(new DataIntegrityViolationException("other unique", oraError));

        assertThatThrownBy(() -> adapter.save(SEARCH))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void nestedSqlExceptionShouldBeDetectedEvenWhenItsOwnCauseIsMoreSpecific() {
        var sql = new SQLException("ORA-00001", "23000", 1,
                new RuntimeException("Oracle driver detail"));
        var constraint = new ConstraintViolationException("duplicate", sql, "\"HOTEL\".\"PK_HOTEL_SEARCH\"");
        var failure = new DataIntegrityViolationException("insert failed", new RuntimeException(constraint));
        when(jpaRepository.insertSearch(any(), any(), any(), any(), any())).thenThrow(failure);
        assertAll(
                () -> assertDoesNotThrow(() -> adapter.save(SEARCH)),
                () -> verify(jpaRepository, never()).findById(any()));
    }

    @Test
    void reportedConstraintNameShouldTakePrecedenceOverSqlMessage() {
        var sql = new SQLException("ORA-00001 (HOTEL.PK_HOTEL_SEARCH)", "23000", 1);
        var constraint = new ConstraintViolationException("duplicate", sql, "OTHER_CONSTRAINT");
        var failure = new DataIntegrityViolationException("insert failed", constraint);
        when(jpaRepository.insertSearch(any(), any(), any(), any(), any())).thenThrow(failure);
        assertAll(
                () -> assertThatThrownBy(() -> adapter.save(SEARCH)).isSameAs(failure),
                () -> verify(jpaRepository, never()).findById(any()));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"hotel-1", "different-hotel"})
    void pkDuplicateShouldBeIgnoredWithoutReadingOrComparingPayload(String hotelId) {
        var incoming = new HotelSearch(SEARCH.searchId(), hotelId, SEARCH.checkIn(), SEARCH.checkOut(), List.of(1, 30));
        var sql = new SQLException("ORA-00001 (HOTEL.PK_HOTEL_SEARCH)", "23000", 1);
        when(jpaRepository.insertSearch(any(), any(), any(), any(), any()))
                .thenThrow(new DataIntegrityViolationException("duplicate", sql));

        assertAll(
                () -> assertDoesNotThrow(() -> adapter.save(incoming)),
                () -> verify(jpaRepository).insertSearch(incoming.searchId(), hotelId,
                        incoming.checkIn(), incoming.checkOut(), "1,30"),
                () -> org.mockito.Mockito.verifyNoMoreInteractions(jpaRepository));
    }

    @Test
    void unclassifiedIntegrityViolationShouldNotBeIgnored() {
        var failure = new DataIntegrityViolationException("Unknown constraint");
        when(jpaRepository.insertSearch(any(), any(), any(), any(), any())).thenThrow(failure);

        assertThatThrownBy(() -> adapter.save(SEARCH)).isSameAs(failure);
    }

    @Test
    void oracleTransientFailureShouldPropagateUnchanged() {
        var failure = new org.springframework.dao.TransientDataAccessResourceException("Connection lost");
        when(jpaRepository.insertSearch(any(), any(), any(), any(), any())).thenThrow(failure);

        assertAll(
                () -> assertThatThrownBy(() -> adapter.save(SEARCH)).isSameAs(failure),
                () -> verify(jpaRepository, never()).findById(any()));
    }

    @Test
    void shouldFetchSearchAndExactCountInOneRepositoryCall() {
        var row = new com.sling.hotel.infrastructure.adapter.out.persistence.repository.SearchCountRow(
                SEARCH.searchId(), SEARCH.hotelId(), SEARCH.checkIn(), SEARCH.checkOut(), "30,29,1,3", 7);
        when(jpaRepository.findWithCount(SEARCH.searchId())).thenReturn(Optional.of(row));

        var result = adapter.findWithCount(SEARCH.searchId()).orElseThrow();

        assertAll(
                () -> assertThat(result.search()).isEqualTo(SEARCH),
                () -> assertThat(result.count()).isEqualTo(7));
        assertAll(
                () -> verify(jpaRepository).findWithCount(SEARCH.searchId()),
                () -> org.mockito.Mockito.verifyNoMoreInteractions(jpaRepository));
    }

    @Test
    void lookupShouldReturnEmptyForMissingId() {
        when(jpaRepository.findWithCount("missing")).thenReturn(Optional.empty());
        assertThat(adapter.findWithCount("missing")).isEmpty();
    }

    @Test
    void failedLookupShouldPropagateUnchanged() {
        var failure = new org.springframework.dao.DataAccessResourceFailureException("Oracle unavailable");
        when(jpaRepository.findWithCount("search-1")).thenThrow(failure);
        assertThatThrownBy(() -> adapter.findWithCount("search-1")).isSameAs(failure);
    }
}
