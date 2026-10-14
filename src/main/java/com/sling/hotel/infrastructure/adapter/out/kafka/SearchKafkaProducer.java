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
        var producerRecord = new ProducerRecord<String, Object>(topic, search.searchId(), message);
        producerRecord.headers().add(CorrelationId.HEADER, correlationId.getBytes(StandardCharsets.UTF_8));
        MDC.put(CorrelationId.MDC_KEY, correlationId);
        try {
            var result = kafkaTemplate.send(producerRecord)
                    .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
            var metadata = result.getRecordMetadata();
            if (log.isDebugEnabled()) {
                log.debug("Kafka publication confirmed: searchId={} correlationId={} topic={} partition={} offset={}",
                        search.searchId(), correlationId, metadata.topic(), metadata.partition(), metadata.offset());
            }
        } catch (ExecutionException e) {
            throw publicationFailure(e.getCause(), search.searchId(), correlationId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishException(publicationContext("Interrupted while publishing search to Kafka",
                    search.searchId(), correlationId), e);
        } catch (TimeoutException e) {
            throw new PublishException(publicationContext("Timed out waiting for Kafka acknowledgment",
                    search.searchId(), correlationId), e);
        } catch (RuntimeException e) {
            throw publicationFailure(e, search.searchId(), correlationId);
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
            if (previous != null) {
                MDC.put(CorrelationId.MDC_KEY, previous);
            }
        }
    }

    private RuntimeException publicationFailure(Throwable failure, String searchId, String correlationId) {
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
            return new PublishException(publicationContext("Failed to publish search to Kafka", searchId, correlationId), failure);
        }
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new IllegalStateException(publicationContext("Unexpected Kafka publication failure", searchId, correlationId), failure);
    }

    private String publicationContext(String message, String searchId, String correlationId) {
        return message + ": searchId=" + searchId + " correlationId=" + correlationId + " topic=" + topic;
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
