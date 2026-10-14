package com.sling.hotel.infrastructure.integration;

import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.application.ports.out.SearchRepositoryPort.SearchCount;
import com.sling.hotel.infrastructure.adapter.out.persistence.SearchPersistenceAdapter;
import com.sling.hotel.infrastructure.adapter.out.persistence.repository.SearchJpaRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.assertj.core.api.Assertions.assertThat;

@Tag("oracle-integration")
@DataJpaTest
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(SearchPersistenceAdapter.class)
class OraclePersistenceIntegrationTest {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23-slim-faststart")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withStartupTimeout(Duration.ofMinutes(5));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", oracle::getJdbcUrl);
        registry.add("spring.datasource.username", oracle::getUsername);
        registry.add("spring.datasource.password", oracle::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "oracle.jdbc.OracleDriver");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
    }

    @Autowired
    private SearchPersistenceAdapter adapter;

    @Autowired
    private SearchJpaRepository jpaRepository;

    private static final HotelSearch SEARCH = new HotelSearch(
            "search-1", "hotel-1",
            LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
            List.of(30, 29, 1, 3)
    );

    @BeforeEach
    @AfterEach
    void cleanUp() {
        jpaRepository.deleteAll();
    }

    @Test
    void shouldInsertNewSearch() {
        adapter.save(SEARCH);

        var found = adapter.findWithCount("search-1").map(SearchCount::search);
        assertAll(
                () -> assertThat(found).isPresent(),
                () -> assertThat(found.get().hotelId()).isEqualTo("hotel-1"),
                () -> assertThat(found.get().ages()).containsExactly(30, 29, 1, 3));
    }

    @Test
    void reprocessingSameSearchIdShouldNotDuplicate() {
        adapter.save(SEARCH);
        adapter.save(SEARCH);

        long count = jpaRepository.count();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void countShouldNotIncreaseAfterReprocessing() {
        adapter.save(SEARCH);
        adapter.save(SEARCH);
        adapter.save(SEARCH);

        long count = adapter.findWithCount(SEARCH.searchId()).orElseThrow().count();
        assertThat(count).isEqualTo(1);
    }

    @Test
    void concurrentInsertsOfSameSearchIdShouldResultInSingleRow() throws Exception {
        int threadCount = 10;
        var latch = new CountDownLatch(1);

        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                futures.add(executor.submit(() -> {
                    latch.await();
                    adapter.save(SEARCH);
                    return null;
                }));
            }

            latch.countDown();

            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        }

        assertAll(
                () -> assertThat(jpaRepository.count()).isEqualTo(1),
                () -> assertThat(adapter.findWithCount(SEARCH.searchId()).map(SearchCount::search)).contains(SEARCH));
    }

    @Test
    void identicalSearchesWithDifferentIdsShouldAllBeCounted() {
        var s1 = new HotelSearch("id-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), List.of(30, 29));
        var s2 = new HotelSearch("id-2", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), List.of(30, 29));
        var s3 = new HotelSearch("id-3", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), List.of(30, 29));

        adapter.save(s1);
        adapter.save(s2);
        adapter.save(s3);

        assertThat(adapter.findWithCount(s1.searchId()).orElseThrow().count()).isEqualTo(3);
    }

    @Test
    void sqlInjectionInHotelIdShouldBeTreatedAsLiteralValue() {
        var malicious = new HotelSearch(
                "sql-inj-1", "'; DROP TABLE hotel_search;--",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                List.of(30));

        adapter.save(malicious);

        var found = adapter.findWithCount("sql-inj-1").map(SearchCount::search);
        assertAll(
                () -> assertThat(found).isPresent(),
                () -> assertThat(found.get().hotelId()).isEqualTo("'; DROP TABLE hotel_search;--"));

        adapter.save(SEARCH);
        assertThat(adapter.findWithCount("search-1").map(SearchCount::search)).isPresent();
    }

    @Test
    void sqlInjectionInSearchIdShouldBeTreatedAsLiteralValue() {
        var malicious = new HotelSearch(
                "' OR '1'='1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                List.of(30));

        adapter.save(malicious);

        var found = adapter.findWithCount("' OR '1'='1").map(SearchCount::search);
        assertAll(
                () -> assertThat(found).isPresent(),
                () -> assertThat(found.get().searchId()).isEqualTo("' OR '1'='1"));
    }

    @Test
    void repeatedAgesShouldBePreserved() {
        var search = new HotelSearch(
                "repeated-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                List.of(5, 5, 5, 5));

        adapter.save(search);

        var found = adapter.findWithCount("repeated-1").map(SearchCount::search);
        assertAll(
                () -> assertThat(found).isPresent(),
                () -> assertThat(found.get().ages()).containsExactly(5, 5, 5, 5));
    }

    @Test
    void searchesWithDifferentAgeOrderShouldNotBeCounted() {
        var s1 = new HotelSearch("id-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), List.of(30, 29));
        var s2 = new HotelSearch("id-2", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), List.of(29, 30));

        adapter.save(s1);
        adapter.save(s2);

        assertAll(
                () -> assertThat(adapter.findWithCount(s1.searchId()).orElseThrow().count()).isEqualTo(1),
                () -> assertThat(adapter.findWithCount(s2.searchId()).orElseThrow().count()).isEqualTo(1));
    }

    @Test
    void maxAges50Of100ShouldBePersistedAndRecovered() {
        var ages = java.util.Collections.nCopies(50, 100);
        var search = new HotelSearch(
                "max-ages-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                ages);

        adapter.save(search);

        var found = adapter.findWithCount("max-ages-1").map(SearchCount::search);
        assertAll(
                () -> assertThat(found).isPresent(),
                () -> assertThat(found.get().ages()).hasSize(50).allMatch(a -> a == 100));

        var entity = jpaRepository.findById("max-ages-1");
        assertAll(
                () -> assertThat(entity).isPresent(),
                () -> assertThat(entity.get().getAges()).hasSize(199));
    }

    @Test
    void unicodeHotelIdOf50CharactersShouldBePersistedCorrectly() {
        var unicodeHotelId = "\u4E2D\u6587\u6D4B\u8BD5".repeat(12) + "\u4E2D\u6587";
        assertThat(unicodeHotelId).hasSize(50);

        var search = new HotelSearch(
                "unicode-1", unicodeHotelId,
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                List.of(30));

        adapter.save(search);

        var found = adapter.findWithCount("unicode-1").map(SearchCount::search);
        assertAll(
                () -> assertThat(found).isPresent(),
                () -> assertThat(found.get().hotelId()).isEqualTo(unicodeHotelId));
    }

    @Test
    void countShouldWorkCorrectlyWithMaxAges() {
        var ages = java.util.Collections.nCopies(50, 100);
        var s1 = new HotelSearch("max-count-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), ages);
        var s2 = new HotelSearch("max-count-2", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), ages);

        adapter.save(s1);
        adapter.save(s2);

        assertThat(adapter.findWithCount(s1.searchId()).orElseThrow().count()).isEqualTo(2);
    }

    @Test
    void concurrentDifferentPayloadsForSameIdShouldBothSucceedAndKeepOnePayload() throws Exception {
        var different = new HotelSearch(SEARCH.searchId(), "other-hotel", SEARCH.checkIn(), SEARCH.checkOut(), List.of(29, 30));
        var start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            List<Future<HotelSearch>> futures = new ArrayList<>();
            for (var search : List.of(SEARCH, different)) {
                futures.add(executor.submit(() -> {
                    start.await();
                    adapter.save(search);
                    return search;
                }));
            }
            start.countDown();
            var first = futures.get(0).get(30, TimeUnit.SECONDS);
            var second = futures.get(1).get(30, TimeUnit.SECONDS);
            assertAll(
                    () -> assertThat(List.of(first, second)).containsExactly(SEARCH, different),
                    () -> assertThat(jpaRepository.count()).isEqualTo(1),
                    () -> assertThat(adapter.findWithCount(SEARCH.searchId()).map(SearchCount::search).orElseThrow()).isIn(SEARCH, different));
        }
    }

    @Autowired
    private org.springframework.transaction.PlatformTransactionManager transactionManager;

    @Autowired
    private jakarta.persistence.EntityManagerFactory entityManagerFactory;

    @Test
    void duplicateIdWithDifferentPayloadShouldKeepFirstRow() {
        adapter.save(SEARCH);
        var different = new HotelSearch(SEARCH.searchId(), "other-hotel", SEARCH.checkIn(), SEARCH.checkOut(), List.of(1));

        adapter.save(different);

        assertAll(
                () -> assertThat(adapter.findWithCount(SEARCH.searchId()).map(SearchCount::search)).contains(SEARCH),
                () -> assertThat(jpaRepository.count()).isEqualTo(1),
                () -> assertThat(adapter.findWithCount(SEARCH.searchId()).orElseThrow().count()).isEqualTo(1));
    }

    @Test
    void duplicateTransactionShouldNotPoisonCallerOrNextInsert() {
        adapter.save(SEARCH);
        var transactions = new org.springframework.transaction.support.TransactionTemplate(transactionManager);
        var next = new HotelSearch("next-id", SEARCH.hotelId(), SEARCH.checkIn(), SEARCH.checkOut(), SEARCH.ages());

        transactions.executeWithoutResult(status -> {
            adapter.save(SEARCH);
            assertThat(status.isRollbackOnly()).isFalse();
            adapter.save(next);
            assertThat(status.isRollbackOnly()).isFalse();
        });

        assertAll(
                () -> assertThat(adapter.findWithCount(next.searchId()).map(SearchCount::search)).contains(next),
                () -> assertThat(jpaRepository.count()).isEqualTo(2));
    }

    @Test
    void combinedLookupShouldPreserveExactIdentityAndUseOneStatement() {
        adapter.save(SEARCH);
        var identical = new HotelSearch("same-payload", SEARCH.hotelId(), SEARCH.checkIn(), SEARCH.checkOut(), SEARCH.ages());
        adapter.save(identical);
        adapter.save(new HotelSearch("reversed", SEARCH.hotelId(), SEARCH.checkIn(), SEARCH.checkOut(), List.of(3, 1, 29, 30)));
        adapter.save(new HotelSearch("repeated", SEARCH.hotelId(), SEARCH.checkIn(), SEARCH.checkOut(), List.of(30, 29, 1, 3, 3)));
        adapter.save(new HotelSearch("case", "HOTEL-1", SEARCH.checkIn(), SEARCH.checkOut(), SEARCH.ages()));
        adapter.save(new HotelSearch("other-date", SEARCH.hotelId(), SEARCH.checkIn().minusDays(1), SEARCH.checkOut(), SEARCH.ages()));
        var statistics = entityManagerFactory.unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
        try {
            statistics.clear();
            var result = adapter.findWithCount(SEARCH.searchId()).orElseThrow();
            assertAll(
                    () -> assertThat(result.search()).isEqualTo(SEARCH),
                    () -> assertThat(result.count()).isEqualTo(2),
                    () -> assertThat(statistics.getPrepareStatementCount()).isEqualTo(1));
        } finally {
            statistics.setStatisticsEnabled(false);
        }
        assertAll(
                () -> assertThat(adapter.findWithCount("missing")).isEmpty(),
                () -> assertThat(adapter.findWithCount("' OR '1'='1")).isEmpty());
    }

    @Test
    void combinedLookupShouldRemainCorrectWithSixConcurrentWriters() throws Exception {
        adapter.save(SEARCH);
        var start = new CountDownLatch(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> writes = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                String id = "parallel-" + i;
                writes.add(executor.submit(() -> {
                    start.await();
                    adapter.save(new HotelSearch(id, SEARCH.hotelId(), SEARCH.checkIn(), SEARCH.checkOut(), SEARCH.ages()));
                    return null;
                }));
            }
            start.countDown();
            for (int i = 0; i < 6; i++) {
                var result = adapter.findWithCount(SEARCH.searchId()).orElseThrow();
                assertAll(
                        () -> assertThat(result.search()).isEqualTo(SEARCH),
                        () -> assertThat(result.count()).isBetween(1L, 7L));
            }
            for (var write : writes) {
                write.get(30, TimeUnit.SECONDS);
            }
        }
        var result = adapter.findWithCount(SEARCH.searchId()).orElseThrow();
        assertAll(
                () -> assertThat(result.count()).isEqualTo(7),
                () -> assertThat(jpaRepository.count()).isEqualTo(7));
        for (int i = 0; i < 6; i++) {
            assertThat(adapter.findWithCount("parallel-" + i).orElseThrow().search())
                    .isEqualTo(new HotelSearch("parallel-" + i, SEARCH.hotelId(), SEARCH.checkIn(), SEARCH.checkOut(), SEARCH.ages()));
        }
    }

    @Test
    void nonPkOracleConstraintFailureShouldPropagateAndLeaveRepositoryUsable() {
        org.junit.jupiter.api.Assertions.assertThrows(org.springframework.dao.DataIntegrityViolationException.class,
                () -> jpaRepository.insertSearch("null-hotel", null, SEARCH.checkIn(), SEARCH.checkOut(), "30"));
        adapter.save(SEARCH);
        assertThat(adapter.findWithCount(SEARCH.searchId()).orElseThrow().search()).isEqualTo(SEARCH);
    }

    @Test
    void duplicateShouldExecuteOnlyInsertWithoutAnAdditionalSelect() {
        adapter.save(SEARCH);
        var statistics = entityManagerFactory.unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
        try {
            statistics.clear();
            adapter.save(SEARCH);
            assertThat(statistics.getPrepareStatementCount()).isEqualTo(1);
        } finally {
            statistics.setStatisticsEnabled(false);
        }
        assertThat(adapter.findWithCount(SEARCH.searchId()).map(SearchCount::search)).contains(SEARCH);
    }
}
