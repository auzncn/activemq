package com.zjx.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.jms.*;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ActivemqApplicationTests {

    @Test
    public void contextLoads() {
    }

    @Test
    public void testQueueProducer() throws JMSException {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = activeMQConnectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("testQueue");
        MessageProducer producer = session.createProducer(queue);
        Message message = session.createTextMessage("这是由我编写并发送的第一条消息");
        producer.send(message);
        producer.close();
        session.close();
        connection.close();
    }

    @Test
    public void testQueueConsumer() throws JMSException, IOException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("testQueue");
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                TextMessage textMessage = (TextMessage) message;
                try {
                    String text = textMessage.getText();
                    System.out.println("接收到消息" + text + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        System.in.read();

        consumer.close();
        session.close();
        connection.close();
    }

    @Test
    public void testTopicProducer() throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("testTopic");
        MessageProducer producer = session.createProducer(topic);
        TextMessage textMessage = session.createTextMessage("发布订阅模式消息测试");
        producer.send(textMessage);
    }

    @Test
    public void testTopicCustomer1() throws JMSException, IOException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("testTopic");
        MessageConsumer consumer = session.createConsumer(topic);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                TextMessage textMessage =(TextMessage)message;
                try {
                    String text = textMessage.getText();
                    System.out.println("订阅消费者1号--------"+text+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
        System.in.read();

        consumer.close();
        session.close();
        connection.close();
    }

    @Test
    public void testTopicCustomer2() throws JMSException, IOException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        Topic topic = session.createTopic("testTopic");
        MessageConsumer consumer = session.createConsumer(topic);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                TextMessage textMessage =(TextMessage)message;
                try {
                    String text = textMessage.getText();
                    System.out.println("订阅消费者2号--------"+text+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
        System.in.read();

        consumer.close();
        session.close();
        connection.close();
    }
}

