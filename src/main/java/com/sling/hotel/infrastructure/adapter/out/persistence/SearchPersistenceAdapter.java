package com.sling.hotel.infrastructure.adapter.out.persistence;

import com.sling.hotel.application.ports.out.SearchRepositoryPort;
import com.sling.hotel.domain.exception.IdempotencyConflictException;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.adapter.out.persistence.mapper.SearchEntityMapper;
import com.sling.hotel.infrastructure.adapter.out.persistence.repository.SearchJpaRepository;
import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Repository;

import java.sql.SQLException;
import java.util.Optional;
import java.util.regex.Pattern;

@Repository
public class SearchPersistenceAdapter implements SearchRepositoryPort {

    private static final Logger log = LoggerFactory.getLogger(SearchPersistenceAdapter.class);
    private static final String PK_CONSTRAINT = "PK_HOTEL_SEARCH";
    private static final Pattern ORACLE_CONSTRAINT = Pattern.compile("\\(([^()]+)\\)");

    private final SearchJpaRepository jpaRepository;

    public SearchPersistenceAdapter(SearchJpaRepository jpaRepository) {
        this.jpaRepository = jpaRepository;
    }

    @Override
    public void save(HotelSearch search) {
        int rowsAffected;
        try {
            rowsAffected = jpaRepository.mergeSearch(
                    search.searchId(),
                    search.hotelId(),
                    search.checkIn(),
                    search.checkOut(),
                    SearchEntityMapper.agesToString(search.ages())
            );
        } catch (DataIntegrityViolationException e) {
            if (isPkDuplicate(e)) {
                verifyWinner(search);
                return;
            }
            throw e;
        }

        if (rowsAffected == 0) {
            verifyWinner(search);
        } else {
            log.debug("Search inserted: searchId={}", search.searchId());
        }
    }

    @Override
    public Optional<HotelSearch> findById(String searchId) {
        return jpaRepository.findById(searchId)
                .map(SearchEntityMapper::toDomain);
    }

    @Override
    public long countIdentical(HotelSearch search) {
        return jpaRepository.countByIdentity(
                search.hotelId(),
                search.checkIn(),
                search.checkOut(),
                SearchEntityMapper.agesToString(search.ages())
        );
    }

    private void verifyWinner(HotelSearch incoming) {
        var existing = jpaRepository.findById(incoming.searchId())
                .map(SearchEntityMapper::toDomain);

        if (existing.isPresent() && existing.get().equals(incoming)) {
            log.debug("Idempotent duplicate ignored: searchId={}", incoming.searchId());
        } else {
            log.debug("Idempotency conflict: searchId={} has different payload in database", incoming.searchId());
            throw new IdempotencyConflictException(incoming.searchId());
        }
    }

    private static boolean isPkDuplicate(DataIntegrityViolationException e) {
        boolean duplicate = false;
        boolean pkInMessage = false;
        String constraintName = null;
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof ConstraintViolationException violation
                    && violation.getConstraintName() != null) {
                constraintName = violation.getConstraintName();
            }
            if (cause instanceof SQLException sql && sql.getErrorCode() == 1) {
                duplicate = true;
                if (sql.getMessage() != null) {
                    var matcher = ORACLE_CONSTRAINT.matcher(sql.getMessage());
                    if (matcher.find()) {
                        pkInMessage |= isPkConstraint(matcher.group(1));
                    }
                }
            }
        }
        return duplicate && (constraintName != null ? isPkConstraint(constraintName) : pkInMessage);
    }

    private static boolean isPkConstraint(String constraintName) {
        String unquoted = constraintName.replace("\"", "").trim();
        return PK_CONSTRAINT.equalsIgnoreCase(unquoted.substring(unquoted.lastIndexOf('.') + 1));
    }
}
