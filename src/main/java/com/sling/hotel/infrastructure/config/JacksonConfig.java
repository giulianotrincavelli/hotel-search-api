package com.sling.hotel.infrastructure.config;

import org.springframework.boot.jackson.autoconfigure.JsonMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.cfg.CoercionAction;
import tools.jackson.databind.cfg.CoercionInputShape;
import tools.jackson.databind.ext.javatime.deser.LocalDateDeserializer;
import tools.jackson.databind.ext.javatime.ser.LocalDateSerializer;
import tools.jackson.databind.module.SimpleModule;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;

@Configuration
public class JacksonConfig {

    static final DateTimeFormatter REST_DATE_FORMAT = DateTimeFormatter.ofPattern("dd/MM/uuuu")
            .withResolverStyle(ResolverStyle.STRICT);

    @Bean
    public JsonMapperBuilderCustomizer strictLocalDateCustomizer() {
        return builder -> {
            var module = new SimpleModule("strict-local-date");
            module.addDeserializer(LocalDate.class, new LocalDateDeserializer(REST_DATE_FORMAT));
            module.addSerializer(LocalDate.class, new LocalDateSerializer(REST_DATE_FORMAT));
            builder.addModule(module);
        };
    }

    @Bean
    public JsonMapperBuilderCustomizer strictIntegerTypesCustomizer() {
        return builder -> {
            builder.disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);
            builder.withCoercionConfig(Integer.class, cfg -> {
                cfg.setCoercion(CoercionInputShape.String, CoercionAction.Fail);
                cfg.setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail);
            });
        };
    }

    @Bean
    public JsonMapperBuilderCustomizer strictStringTypesCustomizer() {
        return builder -> builder.withCoercionConfig(String.class, cfg -> {
            cfg.setCoercion(CoercionInputShape.Integer, CoercionAction.Fail);
            cfg.setCoercion(CoercionInputShape.Float, CoercionAction.Fail);
            cfg.setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail);
        });
    }

    @Bean
    public JsonMapperBuilderCustomizer strictLocalDateNoTimestampsCustomizer() {
        return builder -> builder.withCoercionConfig(LocalDate.class, cfg -> {
            cfg.setCoercion(CoercionInputShape.Integer, CoercionAction.Fail);
            cfg.setCoercion(CoercionInputShape.Float, CoercionAction.Fail);
            cfg.setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail);
        });
    }
}
