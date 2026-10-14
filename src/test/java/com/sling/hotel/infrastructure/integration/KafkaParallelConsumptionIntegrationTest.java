package com.sling.hotel.infrastructure.integration;

import com.sling.hotel.application.ports.out.SearchRepositoryPort;
import com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(partitions = 6, topics = {"parallel-searches", "parallel-searches.DLT"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=parallel-consumption",
        "spring.kafka.listener.concurrency=6",
        "spring.kafka.listener.ack-mode=batch",
        "hotel-search.kafka.partitions=6",
        "hotel-search.kafka.topic=parallel-searches",
        "spring.autoconfigure.exclude=" +
                "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration," +
                "org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration," +
                "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"
})
class KafkaParallelConsumptionIntegrationTest {

    @MockitoBean
    private SearchRepositoryPort repository;

    @Autowired
    private KafkaTemplate<String, Object> template;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Test
    void sixPartitionsShouldEnterPersistenceConcurrentlyOnSixVirtualThreads() throws Exception {
        awaitPartitionAssignment();
        var entered = new CountDownLatch(6);
        var release = new CountDownLatch(1);
        Set<Thread> threads = ConcurrentHashMap.newKeySet();
        Set<String> ids = ConcurrentHashMap.newKeySet();
        List<RecordMetadata> records = new ArrayList<>();
        doAnswer(inv -> {
            com.sling.hotel.domain.model.HotelSearch search = inv.getArgument(0);
            threads.add(Thread.currentThread());
            ids.add(search.searchId());
            entered.countDown();
            if (!release.await(20, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Parallel consumers were not released");
            }
            return null;
        }).when(repository).save(any());

        try {
            for (int partition = 0; partition < 6; partition++) {
                var message = new SearchKafkaMessage("parallel-" + partition, "hotel",
                        "2030-12-29", "2030-12-31", List.of(30));
                records.add(template.send(new ProducerRecord<String, Object>(
                        "parallel-searches", partition, message.searchId(), message))
                        .get(10, TimeUnit.SECONDS).getRecordMetadata());
            }
            assertAll(
                    () -> assertThat(entered.await(15, TimeUnit.SECONDS)).isTrue(),
                    () -> assertThat(threads).hasSize(6).allMatch(Thread::isVirtual),
                    () -> assertThat(ids).containsExactlyInAnyOrder(
                            "parallel-0", "parallel-1", "parallel-2", "parallel-3", "parallel-4", "parallel-5"));
            for (var record : records) {
                var offset = KafkaTestUtils.getCurrentOffset(broker.getBrokersAsString(),
                        "parallel-consumption", record.topic(), record.partition());
                if (offset != null) {
                    assertThat(offset.offset()).isLessThanOrEqualTo(record.offset());
                }
            }
        } finally {
            release.countDown();
        }
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            for (var record : records) {
                var offset = KafkaTestUtils.getCurrentOffset(broker.getBrokersAsString(),
                        "parallel-consumption", record.topic(), record.partition());
                assertThat(offset).isNotNull();
                assertThat(offset.offset()).isGreaterThan(record.offset());
            }
        });
    }

    private void awaitPartitionAssignment() {
        var containers = listeners.getListenerContainers();
        assertThat(containers).hasSize(1);
        var container = (ConcurrentMessageListenerContainer<?, ?>) containers.iterator().next();
        assertThat(container.getConcurrency()).isEqualTo(6);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertAll(
                    () -> assertThat(container.getContainers()).hasSize(6).allSatisfy(child ->
                            assertThat(child.getAssignedPartitions()).hasSize(1)),
                    () -> assertThat(container.getAssignedPartitions()).containsExactlyInAnyOrder(
                    new TopicPartition("parallel-searches", 0),
                    new TopicPartition("parallel-searches", 1),
                    new TopicPartition("parallel-searches", 2),
                    new TopicPartition("parallel-searches", 3),
                    new TopicPartition("parallel-searches", 4),
                            new TopicPartition("parallel-searches", 5)));
        });
    }
}
