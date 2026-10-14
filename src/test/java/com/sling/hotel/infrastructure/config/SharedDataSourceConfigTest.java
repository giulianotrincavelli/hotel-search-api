package com.sling.hotel.infrastructure.config;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration;
import org.springframework.boot.flyway.autoconfigure.FlywayMigrationStrategy;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

import javax.sql.DataSource;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

class SharedDataSourceConfigTest {

    @Test
    void shouldUseOneTenConnectionPoolAndSixKafkaConsumers() {
        assertConfiguration(new ApplicationContextRunner(), 10, 2, 6, 6);
    }

    @Test
    void environmentVariablesShouldOverridePoolAndKafkaDefaults() {
        var runner = new ApplicationContextRunner().withInitializer(context ->
                context.getEnvironment().getPropertySources().addFirst(new SystemEnvironmentPropertySource(
                        "testEnvironment", Map.of("HIKARI_MAX_POOL_SIZE", "8", "HIKARI_MIN_IDLE", "1",
                                "KAFKA_LISTENER_CONCURRENCY", "3", "KAFKA_PARTITIONS", "3"))));
        assertConfiguration(runner, 8, 1, 3, 3);
    }

    private void assertConfiguration(ApplicationContextRunner runner, int maximum, int minimum,
                                     int concurrency, int partitions) {
        runner
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withConfiguration(AutoConfigurations.of(DataSourceAutoConfiguration.class,
                        FlywayAutoConfiguration.class, JacksonAutoConfiguration.class, KafkaAutoConfiguration.class))
                .withUserConfiguration(KafkaConfig.class, JacksonConfig.class)
                .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(FlywayMigrationStrategy.class, () -> flyway -> { })
                .withPropertyValues("spring.kafka.admin.auto-create=false")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var dataSource = context.getBean(DataSource.class);
                    assertThat(dataSource).isInstanceOf(HikariDataSource.class);
                    var pool = (HikariDataSource) dataSource;
                    ConcurrentKafkaListenerContainerFactory<?, ?> factory = context.getBean(
                            "kafkaListenerContainerFactory", ConcurrentKafkaListenerContainerFactory.class);
                    assertAll(
                            () -> assertThat(context.getBeansOfType(DataSource.class)).hasSize(1),
                            () -> assertThat(pool.getMaximumPoolSize()).isEqualTo(maximum),
                            () -> assertThat(pool.getMinimumIdle()).isEqualTo(minimum),
                            () -> assertThat(context.getBean(Flyway.class).getConfiguration().getDataSource())
                                    .isSameAs(dataSource),
                            () -> assertThat(factory.createContainer("configuration-test").getConcurrency()).isEqualTo(concurrency),
                            () -> assertThat(context.getBean("searchesTopic", NewTopic.class).numPartitions()).isEqualTo(partitions),
                            () -> assertThat(context.getBean("dltTopic", NewTopic.class).numPartitions()).isEqualTo(partitions));
                });
    }
}
