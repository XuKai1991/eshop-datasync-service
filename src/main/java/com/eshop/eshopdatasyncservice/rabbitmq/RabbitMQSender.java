package com.eshop.eshopdatasyncservice.rabbitmq;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 消息队列发送类
 */
@Component
public class RabbitMQSender {

    @Autowired
    private AmqpTemplate rabbitTemplate;

    public void send(String queue, String message) {
        this.rabbitTemplate.convertAndSend(queue, message);
    }
}