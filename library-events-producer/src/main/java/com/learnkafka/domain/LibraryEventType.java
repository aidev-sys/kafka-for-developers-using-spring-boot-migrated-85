package com.learnkafka.domain;

import jakarta.persistence.*;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LibraryEventType {

    public static final String LIBRARY_EVENT_QUEUE = "library-event-topic";

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue(name = LIBRARY_EVENT_QUEUE, durable = "true"))
    public void listenLibraryEvent(Object event) {
        System.out.println("Received library event: " + event);
    }

    public void sendLibraryEvent(Object event) {
        rabbitTemplate.convertAndSend(LIBRARY_EVENT_QUEUE, event);
    }

    public enum Type {
        NEW,
        UPDATE
    }
}