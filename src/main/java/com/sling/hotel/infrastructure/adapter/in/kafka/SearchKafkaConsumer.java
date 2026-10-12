package com.sling.hotel.infrastructure.adapter.in.kafka;

import com.sling.hotel.application.ports.in.SaveSearchPort;
import com.sling.hotel.domain.model.HotelSearch;
import com.sling.hotel.infrastructure.logging.CorrelationId;
import com.sling.hotel.infrastructure.messaging.kafka.model.SearchKafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

@Component
public class SearchKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(SearchKafkaConsumer.class);

    private final SaveSearchPort saveSearchPort;

    public SearchKafkaConsumer(SaveSearchPort saveSearchPort) {
        this.saveSearchPort = saveSearchPort;
    }

    @KafkaListener(topics = "${hotel-search.kafka.topic}")
    public void consume(ConsumerRecord<String, SearchKafkaMessage> record) {
        SearchKafkaMessage message = record.value();
        String correlationId = CorrelationId.fromHeaders(record.headers());
        MDC.put(CorrelationId.MDC_KEY, correlationId);
        try {
            if (message == null) {
                throw new InvalidKafkaMessageException("Kafka message value must not be null", null);
            }
            String searchId = message.searchId();
            log.debug("Kafka message received: searchId={} correlationId={} topic={} partition={} offset={} message={}",
                    searchId, correlationId, record.topic(), record.partition(), record.offset(), message);
            HotelSearch search = toDomain(message);
            saveSearchPort.save(search);
            log.debug("Kafka message persisted: searchId={} correlationId={} topic={} partition={} offset={}",
                    search.searchId(), correlationId, record.topic(), record.partition(), record.offset());
        } catch (RuntimeException e) {
            log.error("Kafka processing failed: searchId={} correlationId={} topic={} partition={} offset={}",
                    message == null ? record.key() : message.searchId(), correlationId,
                    record.topic(), record.partition(), record.offset(), e);
            throw e;
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
        }
    }

    private static HotelSearch toDomain(SearchKafkaMessage message) {
        try {
            return new HotelSearch(
                    message.searchId(),
                    message.hotelId(),
                    LocalDate.parse(message.checkIn()),
                    LocalDate.parse(message.checkOut()),
                    message.ages()
            );
        } catch (Exception e) {
            throw new InvalidKafkaMessageException("Invalid Kafka message: " + e.getMessage(), e);
        }
    }
}
