package com.sling.hotel.infrastructure.config;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jdbc.autoconfigure.metrics.DataSourcePoolMetricsAutoConfiguration;
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HikariMetricsTest {

    @Test
    void bootShouldExposeStandardHikariMetrics() throws SQLException {
        var delegate = mock(DataSource.class);
        when(delegate.getConnection()).thenAnswer(inv -> {
            var connection = mock(Connection.class);
            when(connection.isValid(anyInt())).thenReturn(true);
            return connection;
        });
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MetricsAutoConfiguration.class,
                        DataSourcePoolMetricsAutoConfiguration.class))
                .withBean(SimpleMeterRegistry.class, SimpleMeterRegistry::new)
                .withBean(HikariDataSource.class, () -> {
                    var pool = new HikariDataSource();
                    pool.setDataSource(delegate);
                    pool.setPoolName("metrics-test");
                    pool.setMaximumPoolSize(1);
                    pool.setMinimumIdle(1);
                    return pool;
                })
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var pool = context.getBean(HikariDataSource.class);
                    var registry = context.getBean(SimpleMeterRegistry.class);
                    assertThat(pool.getMetricsTrackerFactory()).isInstanceOf(MicrometerMetricsTrackerFactory.class);
                    try (var connection = pool.getConnection()) {
                        assertAll(
                                () -> assertThat(connection.isClosed()).isFalse(),
                                () -> assertThat(registry.get("hikaricp.connections.active").tag("pool", "metrics-test")
                                        .gauge().value()).isEqualTo(1),
                                () -> assertThat(registry.get("hikaricp.connections.pending").gauge().value()).isZero(),
                                () -> assertThat(registry.get("hikaricp.connections.max").gauge().value()).isEqualTo(1));
                    }
                    assertAll(
                            () -> assertThat(registry.get("hikaricp.connections.acquire").timer().count()).isEqualTo(1),
                            () -> assertThat(registry.get("hikaricp.connections.usage").timer().count()).isEqualTo(1),
                            () -> assertThat(registry.get("hikaricp.connections.idle").gauge()).isNotNull());
                });
    }
}
