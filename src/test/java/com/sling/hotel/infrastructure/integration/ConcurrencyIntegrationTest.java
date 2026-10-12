package com.sling.hotel.infrastructure.integration;

import com.jayway.jsonpath.JsonPath;
import com.sling.hotel.infrastructure.adapter.out.persistence.repository.SearchJpaRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Tag("oracle-integration")
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
@EmbeddedKafka(
        partitions = 1,
        topics = {"hotel_availability_searches", "hotel_availability_searches.DLT"}
)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "hotel-search.kafka.topic=hotel_availability_searches",
        "hotel-search.kafka.dlt-suffix=.DLT",
        "hotel-search.kafka.retry.interval=1s",
        "hotel-search.kafka.retry.max-attempts=60",
        "hotel-search.kafka.producer.send-timeout=10s",
        "hotel-search.kafka.dlt.send-timeout=10s"
})
class ConcurrencyIntegrationTest {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23-slim-faststart")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withStartupTimeout(java.time.Duration.ofMinutes(5));

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
    private MockMvc mockMvc;

    @Autowired
    private SearchJpaRepository jpaRepository;

    @BeforeEach
    @AfterEach
    void cleanUp() {
        jpaRepository.deleteAll();
    }

    @Test
    void hundredConcurrentPostsShouldAllSucceedAndPersist() throws Exception {
        int count = 100;
        var barrier = new CyclicBarrier(count);

        try (ExecutorService executor = Executors.newFixedThreadPool(count)) {
            List<Future<MvcResult>> futures = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                final int idx = i;
                futures.add(executor.submit(() -> {
                    barrier.await(30, TimeUnit.SECONDS);
                    return mockMvc.perform(post("/search")
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .content(searchJson("hotel-" + idx, List.of(idx % 101))))
                            .andExpect(status().isOk())
                            .andReturn();
                }));
            }

            var searchIds = new HashSet<String>();
            var expectedHotels = new HashMap<String, String>();
            int index = 0;
            for (Future<MvcResult> future : futures) {
                MvcResult result = future.get(60, TimeUnit.SECONDS);
                String body = result.getResponse().getContentAsString();
                String searchId = JsonPath.read(body, "$.searchId");
                assertThat(searchId).isNotBlank();
                searchIds.add(searchId);
                expectedHotels.put(searchId, "hotel-" + index++);
            }

            assertThat(searchIds).hasSize(count);

            long deadline = System.currentTimeMillis() + 60_000;
            while (jpaRepository.count() < count && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
            assertThat(jpaRepository.count()).isEqualTo(count);
            var persisted = jpaRepository.findAll();
            assertThat(persisted).extracting(e -> e.getSearchId()).containsExactlyInAnyOrderElementsOf(searchIds);
            assertThat(persisted).allSatisfy(e -> {
                assertThat(e.getHotelId()).isEqualTo(expectedHotels.get(e.getSearchId()));
                assertThat(e.getAges()).isEqualTo(e.getHotelId().substring("hotel-".length()));
            });
        }
    }

    @Test
    void agesOrderShouldBeDistinguishedInConcurrentRequests() throws Exception {
        int count = 10;
        var barrier = new CyclicBarrier(count);

        try (ExecutorService executor = Executors.newFixedThreadPool(count)) {
            List<Future<MvcResult>> futures = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                final List<Integer> ages = (i < 5) ? List.of(30, 29) : List.of(29, 30);
                futures.add(executor.submit(() -> {
                    barrier.await(30, TimeUnit.SECONDS);
                    return mockMvc.perform(post("/search")
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .content(searchJson("hotel-1", ages)))
                            .andExpect(status().isOk())
                            .andReturn();
                }));
            }

            var expectedAges = new HashMap<String, String>();
            for (int i = 0; i < futures.size(); i++) {
                var response = futures.get(i).get(60, TimeUnit.SECONDS).getResponse().getContentAsString();
                String id = JsonPath.read(response, "$.searchId");
                expectedAges.put(id, i < 5 ? "30,29" : "29,30");
            }
            assertThat(expectedAges).hasSize(count);

            long deadline = System.currentTimeMillis() + 60_000;
            while (jpaRepository.count() < count && System.currentTimeMillis() < deadline) {
                Thread.sleep(200);
            }
            assertThat(jpaRepository.count()).isEqualTo(count);

            var all = jpaRepository.findAll();
            assertThat(all).extracting(e -> e.getSearchId()).containsExactlyInAnyOrderElementsOf(expectedAges.keySet());
            assertThat(all).allSatisfy(e -> assertThat(e.getAges()).isEqualTo(expectedAges.get(e.getSearchId())));
            var withOrder1 = all.stream().filter(e -> e.getAges().equals("30,29")).count();
            var withOrder2 = all.stream().filter(e -> e.getAges().equals("29,30")).count();
            assertThat(withOrder1).isEqualTo(5);
            assertThat(withOrder2).isEqualTo(5);
        }
    }

    @Test
    void duplicateAgesShouldBePreservedEndToEnd() throws Exception {
        MvcResult result = mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(searchJson("hotel-dup", List.of(5, 5, 5, 5))))
                .andExpect(status().isOk())
                .andReturn();

        String searchId = JsonPath.read(result.getResponse().getContentAsString(), "$.searchId");

        long deadline = System.currentTimeMillis() + 30_000;
        while (jpaRepository.findById(searchId).isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }

        var entity = jpaRepository.findById(searchId);
        assertThat(entity).isPresent();
        assertThat(entity.get().getAges()).isEqualTo("5,5,5,5");
    }

    @Test
    void maxAgesEndToEndShouldPersistAndRecover() throws Exception {
        var ages = Collections.nCopies(50, 100);
        MvcResult result = mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(searchJson("hotel-max", ages)))
                .andExpect(status().isOk())
                .andReturn();

        String searchId = JsonPath.read(result.getResponse().getContentAsString(), "$.searchId");

        long deadline = System.currentTimeMillis() + 30_000;
        while (jpaRepository.findById(searchId).isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }

        var entity = jpaRepository.findById(searchId);
        assertThat(entity).isPresent();
        assertThat(entity.get().getAges()).hasSize(199);

        String countBody = mockMvc.perform(get("/count").param("searchId", searchId))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        List<Integer> returnedAges = JsonPath.read(countBody, "$.search.ages");
        assertThat(returnedAges).hasSize(50).allMatch(a -> a == 100);
        assertThat((int) JsonPath.read(countBody, "$.count")).isEqualTo(1);
    }

    private static String searchJson(String hotelId, List<Integer> ages) {
        return """
                {
                  "hotelId": "%s",
                  "checkIn": "29/12/2023",
                  "checkOut": "31/12/2023",
                  "ages": %s
                }
                """.formatted(hotelId, ages);
    }
}
