package com.learnkafka.service;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
public class FailureService {

    private final FailureRecordRepository failureRecordRepository;
    private final RabbitTemplate rabbitTemplate;

    public FailureService(FailureRecordRepository failureRecordRepository, RabbitTemplate rabbitTemplate) {
        this.failureRecordRepository = failureRecordRepository;
        this.rabbitTemplate = rabbitTemplate;
    }

    @RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue(name = "failure.queue", durable = "true"))
    public void saveFailedRecord(String message) {
        // Assuming message contains serialized FailureRecord data
        // In real implementation, you'd deserialize the message properly
        // For now, we're simulating the logic based on the original Kafka approach
        
        // Placeholder logic - actual deserialization would be needed
        // This is a simplified example for demonstration purposes
        FailureRecord failureRecord = new FailureRecord();
        // Set properties from message (you'll need proper deserialization here)
        
        failureRecordRepository.save(failureRecord);
    }

    public void sendFailureMessage(Object message) {
        rabbitTemplate.convertAndSend("failure.exchange", "failure.routing.key", message);
    }
}