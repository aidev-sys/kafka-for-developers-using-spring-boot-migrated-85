package com.learnkafka.config;

import com.learnkafka.service.FailureService;
import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

@Configuration
@EnableRabbit
@Slf4j
public class LibraryEventsConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String SUCCESS = "SUCCESS";
    public static final String DEAD = "DEAD";

    @Autowired
    LibraryEventsService libraryEventsService;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    FailureService failureService;

    @Value("${topics.retry:library-events.RETRY}")
    private String retryQueue;

    @Value("${topics.dlt:library-events.DLT}")
    private String deadLetterQueue;

    @Bean
    Queue retryQueue() {
        return new Queue(retryQueue, true);
    }

    @Bean
    Queue deadLetterQueue() {
        return new Queue(deadLetterQueue, true);
    }

    @Bean
    DirectExchange retryExchange() {
        return new DirectExchange("retry.exchange");
    }

    @Bean
    DirectExchange deadLetterExchange() {
        return new DirectExchange("dead-letter.exchange");
    }

    @Bean
    Binding retryBinding() {
        return BindingBuilder.bind(retryQueue()).to(retryExchange()).with("retry.routing.key");
    }

    @Bean
    Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue()).to(deadLetterExchange()).with("dead.letter.routing.key");
    }

    @Bean
    MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    @ConditionalOnMissingBean(name = "rabbitListenerContainerFactory")
    SimpleMessageListenerContainer rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(retryQueue, deadLetterQueue);
        container.setConcurrentConsumers(3);
        container.setDefaultRequeueRejected(false);
        return container;
    }

    @RabbitListener(queuesToDeclare = @Queue(name = "library-events.RETRY", durable = "true"))
    public void listenRetry(String message) {
        log.info("Retry message received: {}", message);
        // Process retry logic here
    }

    @RabbitListener(queuesToDeclare = @Queue(name = "library-events.DLT", durable = "true"))
    public void listenDeadLetter(String message) {
        log.info("Dead letter message received: {}", message);
        // Process dead letter logic here
    }

    @Bean
    public FixedBackOff fixedBackOff() {
        FixedBackOff backOff = new FixedBackOff(1000L, 2L);
        return backOff;
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer() {
        return new DeadLetterPublishingRecoverer(rabbitTemplate, (message, exception) -> {
            log.error("Exception in publishingRecoverer : {} ", exception.getMessage(), exception);
            if (exception.getCause() instanceof RecoverableDataAccessException) {
                return new org.springframework.amqp.core.MessageDeliveryMode.Permanent(retryQueue);
            } else {
                return new org.springframework.amqp.core.MessageDeliveryMode.Permanent(deadLetterQueue);
            }
        });
    }

    @Bean
    public MessageListenerAdapter messageListenerAdapter() {
        return new MessageListenerAdapter(this, "handleMessage");
    }

    public void handleMessage(String message) {
        log.info("Received message: {}", message);
        // Handle message logic here
    }
}