package com.sling.hotel.infrastructure.config;

import com.sling.hotel.domain.exception.IdempotencyConflictException;
import com.sling.hotel.infrastructure.adapter.in.kafka.InvalidKafkaMessageException;
import com.sling.hotel.infrastructure.adapter.out.kafka.SearchKafkaProducer;
import com.sling.hotel.infrastructure.logging.CorrelationId;
import com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

@Configuration
@EnableKafka
@EnableConfigurationProperties(HotelSearchKafkaProperties.class)
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    private final HotelSearchKafkaProperties properties;

    public KafkaConfig(HotelSearchKafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    DefaultKafkaProducerFactory<Object, Object> kafkaProducerFactory(
            KafkaProperties kafkaProperties, JsonMapper jsonMapper) {
        var serializer = new DelegatingByTypeSerializer(Map.of(
                SearchKafkaMessage.class, new JacksonJsonSerializer<SearchKafkaMessage>(jsonMapper),
                byte[].class, new ByteArraySerializer()));
        var factory = new DefaultKafkaProducerFactory<Object, Object>(kafkaProperties.buildProducerProperties());
        factory.setValueSerializer(serializer);
        return factory;
    }

    @Bean
    DefaultKafkaConsumerFactory<String, SearchKafkaMessage> kafkaConsumerFactory(
            KafkaProperties kafkaProperties, JsonMapper jsonMapper) {
        var deserializer = new JacksonJsonDeserializer<>(SearchKafkaMessage.class, jsonMapper);
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties(),
                new StringDeserializer(), new ErrorHandlingDeserializer<>(deserializer));
    }

    @Bean
    SearchKafkaProducer searchKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        return new SearchKafkaProducer(
                kafkaTemplate,
                properties.topic(),
                properties.producer().sendTimeout()
        );
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic searchesTopic() {
        return TopicBuilder.name(properties.topic())
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic dltTopic() {
        return TopicBuilder.name(properties.dltTopic())
                .partitions(1)
                .replicas(1)
                .build();
    }

    @SuppressWarnings("unchecked")
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, SearchKafkaMessage> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<String, SearchKafkaMessage> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, SearchKafkaMessage>();
        configurer.configure(
                (ConcurrentKafkaListenerContainerFactory<Object, Object>) (ConcurrentKafkaListenerContainerFactory<?, ?>) factory,
                (ConsumerFactory<Object, Object>) (ConsumerFactory<?, ?>) consumerFactory);
        var executor = new SimpleAsyncTaskExecutor("kafka-");
        executor.setVirtualThreads(true);
        factory.getContainerProperties().setListenerTaskExecutor(executor);
        return factory;
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            KafkaOperations<String, Object> kafkaOperations) {

        var dltTopic = properties.dltTopic();
        var recoverer = new DeadLetterPublishingRecoverer(kafkaOperations,
                (record, ex) -> new TopicPartition(dltTopic, -1));
        recoverer.setWaitForSendResultTimeout(properties.dlt().sendTimeout());
        recoverer.setFailIfSendResultIsError(true);

        var retry = properties.retry();
        var backOff = new FixedBackOff(retry.interval().toMillis(), retry.maxAttempts());
        ConsumerAwareRecordRecoverer correlatedRecoverer = (record, consumer, exception) ->
                CorrelationId.withHeaders(record.headers(), () -> recoverer.accept(record, consumer, exception));
        var errorHandler = new DefaultErrorHandler(correlatedRecoverer, backOff);

        errorHandler.addNotRetryableExceptions(InvalidKafkaMessageException.class);
        errorHandler.addNotRetryableExceptions(IdempotencyConflictException.class);

        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception exception,
                                       int deliveryAttempt) {
                CorrelationId.withHeaders(record.headers(), () ->
                        log.warn("Kafka delivery failed: attempt={} topic={} partition={} offset={} searchId={} correlationId={} exception={}",
                                deliveryAttempt, record.topic(), record.partition(), record.offset(),
                                extractSearchId(record), MDC.get(CorrelationId.MDC_KEY), exception.getClass().getSimpleName()));
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception exception) {
                CorrelationId.withHeaders(record.headers(), () ->
                        log.error("Kafka message recovered to DLT: sourceTopic={} dltTopic={} partition={} offset={} searchId={} correlationId={}",
                                record.topic(), dltTopic, record.partition(), record.offset(),
                                extractSearchId(record), MDC.get(CorrelationId.MDC_KEY), exception));
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original,
                                       Exception failure) {
                CorrelationId.withHeaders(record.headers(), () ->
                        log.error("Kafka DLT recovery failed: topic={} dltTopic={} partition={} offset={} searchId={} correlationId={} originalException={}",
                                record.topic(), dltTopic, record.partition(), record.offset(),
                                extractSearchId(record), MDC.get(CorrelationId.MDC_KEY), original.getClass().getSimpleName(), failure));
            }
        });

        return errorHandler;
    }

    private static String extractSearchId(ConsumerRecord<?, ?> record) {
        if (record.value() instanceof SearchKafkaMessage msg && msg.searchId() != null) {
            return msg.searchId();
        }
        return record.key() instanceof String key ? key : "unknown";
    }
}
