package com.sling.hotel.infrastructure.adapter.out.kafka;

import com.sling.hotel.application.exception.PublishException;
import com.sling.hotel.application.ports.out.SearchPublisherPort;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.logging.CorrelationId;
import com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SearchKafkaProducer implements SearchPublisherPort {

    private static final Logger log = LoggerFactory.getLogger(SearchKafkaProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;
    private final Duration sendTimeout;

    public SearchKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate,
                               String topic, Duration sendTimeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.sendTimeout = sendTimeout;
    }

    @Override
    public void publish(HotelSearch search) {
        var message = toMessage(search);
        String previous = MDC.get(CorrelationId.MDC_KEY);
        String correlationId = CorrelationId.resolve(previous);
        var record = new ProducerRecord<String, Object>(topic, search.searchId(), message);
        record.headers().add(CorrelationId.HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
        MDC.put(CorrelationId.MDC_KEY, correlationId);
        try {
            var result = kafkaTemplate.send(record)
                    .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
            var metadata = result.getRecordMetadata();
            log.debug("Kafka publication confirmed: searchId={} correlationId={} topic={} partition={} offset={}",
                    search.searchId(), correlationId, metadata.topic(), metadata.partition(), metadata.offset());
        } catch (ExecutionException e) {
            log.error("Kafka publication failed: searchId={} correlationId={} topic={}",
                    search.searchId(), correlationId, topic, e.getCause());
            throw publicationFailure(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Kafka publication interrupted: searchId={} correlationId={} topic={}",
                    search.searchId(), correlationId, topic, e);
            throw new PublishException("Interrupted while publishing search to Kafka", e);
        } catch (TimeoutException e) {
            log.error("Kafka publication timed out: searchId={} correlationId={} topic={}",
                    search.searchId(), correlationId, topic, e);
            throw new PublishException("Timed out waiting for Kafka acknowledgment", e);
        } catch (RuntimeException e) {
            log.error("Kafka publication failed: searchId={} correlationId={} topic={}",
                    search.searchId(), correlationId, topic, e);
            throw publicationFailure(e);
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
            if (previous != null) {
                MDC.put(CorrelationId.MDC_KEY, previous);
            }
        }
    }

    private static RuntimeException publicationFailure(Throwable failure) {
        Throwable cause = failure;
        boolean kafkaFailure = false;
        while (true) {
            if (cause instanceof SerializationException || cause instanceof ConfigException) {
                return (RuntimeException) cause;
            }
            kafkaFailure |= cause instanceof KafkaException;
            if (cause.getCause() == null || !(cause instanceof KafkaException
                    || cause instanceof org.springframework.kafka.KafkaException
                    || cause instanceof CompletionException)) {
                break;
            }
            cause = cause.getCause();
        }
        if (kafkaFailure) {
            return new PublishException("Failed to publish search to Kafka", failure);
        }
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new IllegalStateException("Unexpected Kafka publication failure", failure);
    }

    private static SearchKafkaMessage toMessage(HotelSearch search) {
        return new SearchKafkaMessage(
                search.searchId(),
                search.hotelId(),
                search.checkIn().toString(),
                search.checkOut().toString(),
                search.ages()
        );
    }
}
