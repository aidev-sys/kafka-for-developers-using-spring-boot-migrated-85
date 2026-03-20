package com.learnkafka.entity;

import jakarta.persistence.*;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Component;

@Component
public enum LibraryEventType {

    NEW,
    UPDATE;

    @RabbitListener(queues = "library.event.queue")
    public void processLibraryEvent(Object event) {
        System.out.println("Processing library event: " + event);
    }

    public static class Producer {
        @Autowired
        private RabbitTemplate rabbitTemplate;

        public void sendLibraryEvent(Object event) {
            rabbitTemplate.convertAndSend("library.event.queue", event);
        }
    }

    public static class Consumer {
        @RabbitListener(queues = "library.event.queue")
        public void receiveLibraryEvent(Object event) {
            System.out.println("Received library event: " + event);
        }
    }

    public static class QueueConfig {
        public Queue libraryEventQueue() {
            return new Queue("library.event.queue", true);
        }
    }
}