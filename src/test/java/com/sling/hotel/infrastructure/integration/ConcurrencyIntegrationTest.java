package com.sling.hotel.infrastructure.integration;

import com.jayway.jsonpath.JsonPath;
import com.sling.hotel.infrastructure.adapter.out.persistence.repository.SearchJpaRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.bean.override.convention.TestBean;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

import java.time.Duration;
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

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Tag("oracle-integration")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureMockMvc
@Testcontainers
@EmbeddedKafka(
        partitions = 1,
        topics = {"hotel_availability_searches", "hotel_availability_searches.DLT"}
)
@TestPropertySource(properties = {
        "hotel-search.kafka.partitions=1",
        "spring.kafka.listener.concurrency=1",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "hotel-search.kafka.topic=hotel_availability_searches",
        "hotel-search.kafka.dlt-suffix=.DLT",
        "hotel-search.kafka.retry.interval=1s",
        "hotel-search.kafka.retry.max-attempts=60",
        "hotel-search.kafka.producer.send-timeout=10s",
        "hotel-search.kafka.dlt.send-timeout=10s"
})
class ConcurrencyIntegrationTest {

    @TestBean
    private Clock clock;

    static Clock clock() {
        return Clock.fixed(Instant.parse("2026-10-13T12:00:00Z"), ZoneOffset.UTC);
    }

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
                            .andExpect(status().isAccepted())
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

            await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(jpaRepository.count()).isEqualTo(count));
            var persisted = jpaRepository.findAll();
            assertAll(
                    () -> assertThat(persisted).extracting(e -> e.getSearchId()).containsExactlyInAnyOrderElementsOf(searchIds),
                    () -> assertThat(persisted).allSatisfy(e -> {
                assertAll(
                        () -> assertThat(e.getHotelId()).isEqualTo(expectedHotels.get(e.getSearchId())),
                        () -> assertThat(e.getAges()).isEqualTo(e.getHotelId().substring("hotel-".length())));
            }));
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
                            .andExpect(status().isAccepted())
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

            await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                    assertThat(jpaRepository.count()).isEqualTo(count));

            var all = jpaRepository.findAll();
            assertAll(
                    () -> assertThat(all).extracting(e -> e.getSearchId()).containsExactlyInAnyOrderElementsOf(expectedAges.keySet()),
                    () -> assertThat(all).allSatisfy(e -> assertThat(e.getAges()).isEqualTo(expectedAges.get(e.getSearchId()))));
            var withOrder1 = all.stream().filter(e -> e.getAges().equals("30,29")).count();
            var withOrder2 = all.stream().filter(e -> e.getAges().equals("29,30")).count();
            assertAll(
                    () -> assertThat(withOrder1).isEqualTo(5),
                    () -> assertThat(withOrder2).isEqualTo(5));
        }
    }

    @Test
    void duplicateAgesShouldBePreservedEndToEnd() throws Exception {
        MvcResult result = mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(searchJson("hotel-dup", List.of(5, 5, 5, 5))))
                .andExpect(status().isAccepted())
                .andReturn();

        String searchId = JsonPath.read(result.getResponse().getContentAsString(), "$.searchId");

        await().atMost(Duration.ofSeconds(30)).until(() -> jpaRepository.existsById(searchId));

        var entity = jpaRepository.findById(searchId);
        assertAll(
                () -> assertThat(entity).isPresent(),
                () -> assertThat(entity.get().getAges()).isEqualTo("5,5,5,5"));
    }

    @Test
    void maxAgesEndToEndShouldPersistAndRecover() throws Exception {
        var ages = Collections.nCopies(50, 100);
        MvcResult result = mockMvc.perform(post("/search")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(searchJson("hotel-max", ages)))
                .andExpect(status().isAccepted())
                .andReturn();

        String searchId = JsonPath.read(result.getResponse().getContentAsString(), "$.searchId");

        await().atMost(Duration.ofSeconds(30)).until(() -> jpaRepository.existsById(searchId));

        var entity = jpaRepository.findById(searchId);
        assertAll(
                () -> assertThat(entity).isPresent(),
                () -> assertThat(entity.get().getAges()).hasSize(199));

        String countBody = mockMvc.perform(get("/count").param("searchId", searchId))
                .andExpect(status().isOk())
                .andReturn()
                .getResponse()
                .getContentAsString();

        List<Integer> returnedAges = JsonPath.read(countBody, "$.search.ages");
        assertAll(
                () -> assertThat(returnedAges).hasSize(50).allMatch(a -> a == 100),
                () -> assertThat((int) JsonPath.read(countBody, "$.count")).isEqualTo(1));
    }

    @Test
    void identicalPostsShouldHaveUniqueIdsAndEventuallyCountWithoutSortingAges() throws Exception {
        var ids = new ArrayList<String>();
        for (int i = 0; i < 3; i++) {
            var response = mockMvc.perform(post("/search").contentType(MediaType.APPLICATION_JSON)
                            .content(searchJson("hotel-e2e", List.of(30, 29))))
                    .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
            ids.add(JsonPath.read(response, "$.searchId"));
        }
        var reversed = mockMvc.perform(post("/search").contentType(MediaType.APPLICATION_JSON)
                        .content(searchJson("hotel-e2e", List.of(29, 30))))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString();
        String reversedId = JsonPath.read(reversed, "$.searchId");
        assertThat(new HashSet<>(ids)).hasSize(3).doesNotContain(reversedId);
        for (var id : ids) {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    mockMvc.perform(get("/count").param("searchId", id))
                            .andExpect(status().isOk())
                            .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.count").value(3))
                            .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.search.ages[0]").value(30))
                            .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.search.ages[1]").value(29)));
        }
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                mockMvc.perform(get("/count").param("searchId", reversedId))
                        .andExpect(status().isOk())
                        .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.count").value(1))
                        .andExpect(org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath("$.search.ages[0]").value(29)));
    }

    private static String searchJson(String hotelId, List<Integer> ages) {
        return """
                {
                  "hotelId": "%s",
                  "checkIn": "29/12/2030",
                  "checkOut": "31/12/2030",
                  "ages": %s
                }
                """.formatted(hotelId, ages);
    }

    @Autowired
    private org.springframework.kafka.core.KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private org.springframework.kafka.test.EmbeddedKafkaBroker broker;

    @Test
    void oracleRedeliveryWithDifferentPayloadShouldCommitWithoutChangingRowOrPublishingDlt() throws Exception {
        var original = new com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage(
                "oracle-redelivery", "original-hotel", "2030-12-29", "2030-12-31", List.of(30, 1, 30));
        var changed = new com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage(
                original.searchId(), "changed-hotel", original.checkIn(), original.checkOut(), List.of(1));
        var dlt = new org.apache.kafka.common.TopicPartition("hotel_availability_searches.DLT", 0);
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(java.util.Map.of(
                org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()))) {
            long dltBefore = admin.listOffsets(java.util.Map.of(dlt, org.apache.kafka.clients.admin.OffsetSpec.latest()))
                    .all().get(10, TimeUnit.SECONDS).get(dlt).offset();
            for (var message : List.of(original, changed, original)) {
                var sent = kafkaTemplate.send("hotel_availability_searches", message.searchId(), message)
                        .get(10, TimeUnit.SECONDS).getRecordMetadata();
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    var offset = org.springframework.kafka.test.utils.KafkaTestUtils.getCurrentOffset(
                            broker.getBrokersAsString(), "hotel-search-group", sent.topic(), sent.partition());
                    assertThat(offset).isNotNull();
                    assertThat(offset.offset()).isGreaterThan(sent.offset());
                });
            }
            var row = jpaRepository.findWithCount(original.searchId()).orElseThrow();
            long dltAfter = admin.listOffsets(java.util.Map.of(dlt, org.apache.kafka.clients.admin.OffsetSpec.latest()))
                    .all().get(10, TimeUnit.SECONDS).get(dlt).offset();
            assertAll(
                    () -> assertThat(row.hotelId()).isEqualTo(original.hotelId()),
                    () -> assertThat(row.ages()).isEqualTo("30,1,30"),
                    () -> assertThat(row.count()).isEqualTo(1),
                    () -> assertThat(jpaRepository.count()).isEqualTo(1),
                    () -> assertThat(dltAfter).isEqualTo(dltBefore));
        }
    }
}
