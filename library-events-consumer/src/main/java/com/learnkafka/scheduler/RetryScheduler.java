package com.learnkafka.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.config.LibraryEventsConsumerConfig;
import com.learnkafka.consumer.LibraryEventsConsumer;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.service.LibraryEventsService;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@ConditionalOnProperty(name = "app.retry.scheduler.enabled", havingValue = "true", matchIfMissing = true)
public class RetryScheduler {

    @Autowired
    LibraryEventsService libraryEventsService;

    @Autowired
    FailureRecordRepository failureRecordRepository;

    @PersistenceContext
    EntityManager entityManager;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Scheduled(fixedRate = 10000)
    public void retryFailedRecords(){
        log.info("Retrying Failed Records Started!");
        var status = LibraryEventsConsumerConfig.RETRY;
        List<FailureRecord> failureRecords = failureRecordRepository.findAllByStatus(status);
        for (FailureRecord failureRecord : failureRecords) {
            try {
                var consumerRecord = buildConsumerRecord(failureRecord);
                libraryEventsService.processLibraryEvent(consumerRecord);
                failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
                entityManager.merge(failureRecord);
            } catch (Exception e){
                log.error("Exception in retryFailedRecords : ", e);
            }
        }
    }

    private org.springframework.kafka.clients.consumer.ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new org.springframework.kafka.clients.consumer.ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffset_value(),
                failureRecord.getKey(),
                failureRecord.getErrorRecord()
        );
    }

    @RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue(name = "retry.queue", durable = "true"))
    public void handleRetry(String message) {
        log.info("Handling retry message: {}", message);
    }

    public void sendToRetryQueue(Object message) {
        rabbitTemplate.convertAndSend("retry.queue", message);
    }
}