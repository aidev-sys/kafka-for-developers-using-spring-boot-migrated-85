package com.learnkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.*;

@Component
@Slf4j
public class LibraryEventsConsumerManualOffset {

    @RabbitListener(queuesToDeclare = @org.springframework.amqp.rabbit.annotation.Queue(name = "library-events", durable = "true"))
    public void onMessage(String message, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {
        log.info("Message received: {}", message);
        // Simulate processing and acknowledge via manual ack in RabbitMQ
    }
}