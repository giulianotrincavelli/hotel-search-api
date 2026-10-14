package com.sling.hotel.infrastructure.config;

import com.sling.hotel.infrastructure.adapter.in.kafka.InvalidKafkaMessageException;
import com.sling.hotel.infrastructure.adapter.out.kafka.SearchKafkaProducer;
import com.sling.hotel.infrastructure.logging.CorrelationId;
import com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
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
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.core.MicrometerProducerListener;
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
    private final int partitions;

    public KafkaConfig(HotelSearchKafkaProperties properties,
                       @Value("${hotel-search.kafka.partitions:6}") int partitions) {
        if (partitions < 1) {
            throw new IllegalArgumentException("Kafka partitions must be positive");
        }
        this.properties = properties;
        this.partitions = partitions;
    }

    @Bean
    DefaultKafkaProducerFactory<Object, Object> kafkaProducerFactory(
            KafkaProperties kafkaProperties, JsonMapper jsonMapper, MeterRegistry registry) {
        var serializer = new DelegatingByTypeSerializer(Map.of(
                SearchKafkaMessage.class, new JacksonJsonSerializer<SearchKafkaMessage>(jsonMapper),
                byte[].class, new ByteArraySerializer()));
        var factory = new DefaultKafkaProducerFactory<Object, Object>(kafkaProperties.buildProducerProperties());
        factory.setValueSerializer(serializer);
        factory.addListener(new MicrometerProducerListener<>(registry));
        return factory;
    }

    @Bean
    DefaultKafkaConsumerFactory<String, SearchKafkaMessage> kafkaConsumerFactory(
            KafkaProperties kafkaProperties, JsonMapper jsonMapper, MeterRegistry registry) {
        var deserializer = new JacksonJsonDeserializer<>(SearchKafkaMessage.class, jsonMapper);
        var factory = new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties(),
                new StringDeserializer(), new ErrorHandlingDeserializer<>(deserializer));
        factory.addListener(new MicrometerConsumerListener<>(registry));
        return factory;
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
                .partitions(partitions)
                .replicas(1)
                .build();
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic dltTopic() {
        return TopicBuilder.name(properties.dltTopic())
                .partitions(partitions)
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
        factory.setBatchListener(false);
        factory.getContainerProperties().setSyncCommits(true);
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
                (kafkaRecord, ex) -> new TopicPartition(dltTopic, -1));
        recoverer.setWaitForSendResultTimeout(properties.dlt().sendTimeout());
        recoverer.setFailIfSendResultIsError(true);

        var retry = properties.retry();
        var backOff = new FixedBackOff(retry.interval().toMillis(), retry.maxAttempts());
        ConsumerAwareRecordRecoverer correlatedRecoverer = (kafkaRecord, consumer, exception) ->
                CorrelationId.withHeaders(kafkaRecord.headers(), () -> recoverer.accept(kafkaRecord, consumer, exception));
        var errorHandler = new DefaultErrorHandler(correlatedRecoverer, backOff);

        errorHandler.addNotRetryableExceptions(InvalidKafkaMessageException.class);

        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> kafkaRecord, Exception exception,
                                       int deliveryAttempt) {
                CorrelationId.withHeaders(kafkaRecord.headers(), () ->
                        log.warn("Kafka delivery failed: attempt={} topic={} partition={} offset={} searchId={} correlationId={} exception={}",
                                deliveryAttempt, kafkaRecord.topic(), kafkaRecord.partition(), kafkaRecord.offset(),
                                extractSearchId(kafkaRecord), MDC.get(CorrelationId.MDC_KEY), exception.getClass().getSimpleName(), exception));
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> kafkaRecord, Exception exception) {
                CorrelationId.withHeaders(kafkaRecord.headers(), () ->
                        log.error("Kafka message recovered to DLT: sourceTopic={} dltTopic={} partition={} offset={} searchId={} correlationId={}",
                                kafkaRecord.topic(), dltTopic, kafkaRecord.partition(), kafkaRecord.offset(),
                                extractSearchId(kafkaRecord), MDC.get(CorrelationId.MDC_KEY), exception));
            }

            @Override
            public void recoveryFailed(ConsumerRecord<?, ?> kafkaRecord, Exception original,
                                       Exception failure) {
                CorrelationId.withHeaders(kafkaRecord.headers(), () ->
                        log.error("Kafka DLT recovery failed: topic={} dltTopic={} partition={} offset={} searchId={} correlationId={} originalException={}",
                                kafkaRecord.topic(), dltTopic, kafkaRecord.partition(), kafkaRecord.offset(),
                                extractSearchId(kafkaRecord), MDC.get(CorrelationId.MDC_KEY), original.getClass().getSimpleName(), failure));
            }
        });

        return errorHandler;
    }

    private static String extractSearchId(ConsumerRecord<?, ?> kafkaRecord) {
        if (kafkaRecord.value() instanceof SearchKafkaMessage msg && msg.searchId() != null) {
            return msg.searchId();
        }
        return kafkaRecord.key() instanceof String key ? key : "unknown";
    }
}
