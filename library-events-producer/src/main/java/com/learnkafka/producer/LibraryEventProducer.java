package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
public class LibraryEventProducer {

    RabbitTemplate rabbitTemplate;
    ObjectMapper objectMapper;

    public LibraryEventProducer(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper) {
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        String value = objectMapper.writeValueAsString(libraryEvent);
        CorrelationData correlationData = new CorrelationData(String.valueOf(libraryEvent.getLibraryEventId()));
        rabbitTemplate.convertAndSend("library-events", value, message -> {
            MessageProperties messageProperties = message.getMessageProperties();
            messageProperties.setHeader("event-source", "scanner");
            return message;
        }, correlationData);
    }

    public CompletableFuture<String> sendLibraryEvent_Approach2(LibraryEvent libraryEvent) throws JsonProcessingException {
        String value = objectMapper.writeValueAsString(libraryEvent);
        CorrelationData correlationData = new CorrelationData(String.valueOf(libraryEvent.getLibraryEventId()));
        CompletableFuture<String> completableFuture = new CompletableFuture<>();
        rabbitTemplate.convertAndSend("library-events", value, message -> {
            MessageProperties messageProperties = message.getMessageProperties();
            messageProperties.setHeader("event-source", "scanner");
            return message;
        }, correlationData);
        completableFuture.complete("success");
        return completableFuture;
    }

    public String sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        String value = objectMapper.writeValueAsString(libraryEvent);
        CorrelationData correlationData = new CorrelationData(String.valueOf(libraryEvent.getLibraryEventId()));
        Message message = new Message(value.getBytes(), new MessageProperties());
        message.getMessageProperties().setHeader("event-source", "scanner");
        Object result = rabbitTemplate.convertSendAndReceive("library-events", message, correlationData);
        return (String) result;
    }
}