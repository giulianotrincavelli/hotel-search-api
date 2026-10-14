package com.sling.hotel.infrastructure.adapter.out.persistence;

import com.sling.hotel.infrastructure.adapter.out.persistence.entity.SearchEntity;
import com.sling.hotel.infrastructure.adapter.out.persistence.repository.SearchCountRow;
import com.sling.hotel.infrastructure.adapter.out.persistence.repository.SearchJpaRepository;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.junit.jupiter.api.Test;
import org.springframework.data.jpa.repository.Query;

import static org.assertj.core.api.Assertions.assertThat;

class SearchCountQueryTest {

    @Test
    void hibernateShouldResolveTheCorrelatedCountAndItsTypedProjectionWithOracleDialect() throws Exception {
        String hql = SearchJpaRepository.class.getMethod("findWithCount", String.class)
                .getAnnotation(Query.class).value();
        try (var factory = new Configuration()
                .addAnnotatedClass(SearchEntity.class)
                .setProperty("jakarta.persistence.database-product-name", "Oracle")
                .setProperty("jakarta.persistence.database-major-version", "23")
                .setProperty("jakarta.persistence.database-minor-version", "0")
                .setProperty("hibernate.boot.allow_jdbc_metadata_access", "false")
                .setProperty("hibernate.hbm2ddl.auto", "none")
                .buildSessionFactory()) {
            var parsed = factory.unwrap(SessionFactoryImplementor.class).getQueryEngine().getHqlTranslator()
                    .translate(hql, SearchCountRow.class);
            assertThat(parsed).isInstanceOf(SqmSelectStatement.class);
        }
    }
}
