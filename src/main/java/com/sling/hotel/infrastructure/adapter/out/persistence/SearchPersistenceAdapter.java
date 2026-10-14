package com.sling.hotel.infrastructure.adapter.out.persistence;

import com.sling.hotel.application.ports.out.SearchRepositoryPort;
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
        try {
            jpaRepository.insertSearch(
                    search.searchId(), search.hotelId(), search.checkIn(), search.checkOut(),
                    SearchEntityMapper.agesToString(search.ages()));
        } catch (DataIntegrityViolationException e) {
            if (!isPkDuplicate(e)) {
                throw e;
            }
            if (log.isDebugEnabled()) {
                log.debug("Duplicate searchId ignored: searchId={}", search.searchId());
            }
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Search inserted: searchId={}", search.searchId());
        }
    }

    @Override
    public Optional<SearchCount> findWithCount(String searchId) {
        return jpaRepository.findWithCount(searchId)
                .map(row -> new SearchCount(new HotelSearch(
                        row.searchId(), row.hotelId(), row.checkIn(), row.checkOut(),
                        SearchEntityMapper.stringToAges(row.ages())), row.count()));
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
                pkInMessage |= messageNamesPk(sql.getMessage());
            }
        }
        return duplicate && (constraintName != null ? isPkConstraint(constraintName) : pkInMessage);
    }

    private static boolean messageNamesPk(String message) {
        if (message == null) {
            return false;
        }
        var matcher = ORACLE_CONSTRAINT.matcher(message);
        return matcher.find() && isPkConstraint(matcher.group(1));
    }

    private static boolean isPkConstraint(String constraintName) {
        String unquoted = constraintName.replace("\"", "").trim();
        return PK_CONSTRAINT.equalsIgnoreCase(unquoted.substring(unquoted.lastIndexOf('.') + 1));
    }
}
