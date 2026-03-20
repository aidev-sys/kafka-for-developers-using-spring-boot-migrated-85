package com.learnkafka.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.core.Queue;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
@Table(name = "book")
public class Book {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer bookId;
    
    @NotBlank
    private String bookName;
    
    @NotBlank
    private String bookAuthor;
    
    @OneToOne
    @JoinColumn(name = "library_event_id")
    private LibraryEvent libraryEvent;
    
    public void sendToRabbitMQ(RabbitTemplate rabbitTemplate) {
        rabbitTemplate.convertAndSend("book.queue", this);
    }
    
    @RabbitListener(queuesToDeclare = @Queue(name = "book.queue", durable = "true"))
    public void listenToBookQueue(Object message) {
        // Handle incoming message
    }
}