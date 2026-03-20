package com.learnkafka.jpa;

import com.learnkafka.entity.FailureRecord;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import jakarta.persistence.*;
import java.util.List;

@Repository
public class FailureRecordRepository {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    public void save(FailureRecord failureRecord) {
        entityManager.persist(failureRecord);
    }

    public List<FailureRecord> findAllByStatus(String status) {
        TypedQuery<FailureRecord> query = entityManager.createQuery(
            "SELECT f FROM FailureRecord f WHERE f.status = :status", FailureRecord.class);
        query.setParameter("status", status);
        return query.getResultList();
    }

    @RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue(name = "failure.record.queue", durable = "true"))
    public void handleFailureRecord(Object message) {
        // Process the message
        rabbitTemplate.convertAndSend("failure.record.processed", message);
    }
}