package com.sling.hotel.infrastructure.integration;

import com.sling.hotel.domain.exception.IdempotencyConflictException;
import com.sling.hotel.domain.model.HotelSearch;
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
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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

        var found = adapter.findById("search-1");
        assertThat(found).isPresent();
        assertThat(found.get().hotelId()).isEqualTo("hotel-1");
        assertThat(found.get().ages()).containsExactly(30, 29, 1, 3);
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

        long count = adapter.countIdentical(SEARCH);
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

        assertThat(jpaRepository.count()).isEqualTo(1);
        assertThat(adapter.findById(SEARCH.searchId())).contains(SEARCH);
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

        assertThat(adapter.countIdentical(s1)).isEqualTo(3);
    }

    @Test
    void sqlInjectionInHotelIdShouldBeTreatedAsLiteralValue() {
        var malicious = new HotelSearch(
                "sql-inj-1", "'; DROP TABLE hotel_search;--",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                List.of(30));

        adapter.save(malicious);

        var found = adapter.findById("sql-inj-1");
        assertThat(found).isPresent();
        assertThat(found.get().hotelId()).isEqualTo("'; DROP TABLE hotel_search;--");

        adapter.save(SEARCH);
        assertThat(adapter.findById("search-1")).isPresent();
    }

    @Test
    void sqlInjectionInSearchIdShouldBeTreatedAsLiteralValue() {
        var malicious = new HotelSearch(
                "' OR '1'='1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                List.of(30));

        adapter.save(malicious);

        var found = adapter.findById("' OR '1'='1");
        assertThat(found).isPresent();
        assertThat(found.get().searchId()).isEqualTo("' OR '1'='1");
    }

    @Test
    void repeatedAgesShouldBePreserved() {
        var search = new HotelSearch(
                "repeated-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                List.of(5, 5, 5, 5));

        adapter.save(search);

        var found = adapter.findById("repeated-1");
        assertThat(found).isPresent();
        assertThat(found.get().ages()).containsExactly(5, 5, 5, 5);
    }

    @Test
    void searchesWithDifferentAgeOrderShouldNotBeCounted() {
        var s1 = new HotelSearch("id-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), List.of(30, 29));
        var s2 = new HotelSearch("id-2", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31), List.of(29, 30));

        adapter.save(s1);
        adapter.save(s2);

        assertThat(adapter.countIdentical(s1)).isEqualTo(1);
        assertThat(adapter.countIdentical(s2)).isEqualTo(1);
    }

    @Test
    void maxAges50Of100ShouldBePersistedAndRecovered() {
        var ages = java.util.Collections.nCopies(50, 100);
        var search = new HotelSearch(
                "max-ages-1", "hotel-1",
                LocalDate.of(2023, 12, 29), LocalDate.of(2023, 12, 31),
                ages);

        adapter.save(search);

        var found = adapter.findById("max-ages-1");
        assertThat(found).isPresent();
        assertThat(found.get().ages()).hasSize(50).allMatch(a -> a == 100);

        // Verify the serialized ages string is exactly 199 chars: 50 * "100" + 49 commas
        var entity = jpaRepository.findById("max-ages-1");
        assertThat(entity).isPresent();
        assertThat(entity.get().getAges()).hasSize(199);
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

        var found = adapter.findById("unicode-1");
        assertThat(found).isPresent();
        assertThat(found.get().hotelId()).isEqualTo(unicodeHotelId);
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

        assertThat(adapter.countIdentical(s1)).isEqualTo(2);
    }
    @Test
    void concurrentDifferentPayloadsForSameIdShouldKeepWinnerAndRejectLoser() throws Exception {
        var different = new HotelSearch(SEARCH.searchId(), "other-hotel", SEARCH.checkIn(), SEARCH.checkOut(), List.of(29, 30));
        var start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(2)) {
            List<Future<HotelSearch>> futures = new ArrayList<>();
            for (var search : List.of(SEARCH, different)) {
                futures.add(executor.submit(() -> {
                    start.await();
                    try {
                        adapter.save(search);
                        return search;
                    } catch (IdempotencyConflictException expected) {
                        return null;
                    }
                }));
            }
            start.countDown();
            var first = futures.get(0).get(30, TimeUnit.SECONDS);
            var second = futures.get(1).get(30, TimeUnit.SECONDS);
            assertThat(Stream.of(first, second).filter(Objects::nonNull).toList()).hasSize(1);
            assertThat(jpaRepository.count()).isEqualTo(1);
            assertThat(adapter.findById(SEARCH.searchId())).contains(first != null ? first : second);
        }
    }
}
