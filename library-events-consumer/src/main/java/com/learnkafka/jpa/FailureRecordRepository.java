package com.learnkafka.jpa;

import com.learnkafka.entity.FailureRecord;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Repository;

import jakarta.persistence.*;
import java.util.List;

@Repository
public class FailureRecordRepository {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private StreamBridge streamBridge;

    public void save(FailureRecord failureRecord) {
        entityManager.persist(failureRecord);
    }

    public List<FailureRecord> findAllByStatus(String status) {
        TypedQuery<FailureRecord> query = entityManager.createQuery(
            "SELECT f FROM FailureRecord f WHERE f.status = :status", FailureRecord.class);
        query.setParameter("status", status);
        return query.getResultList();
    }

    @RabbitListener(queues = "failure.record.queue")
    public void handleFailureRecord(Message message) {
        // Process the message
        streamBridge.send("failureRecordSink", message);
    }
}