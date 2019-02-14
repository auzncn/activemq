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
        ConnectionFactory connectionFactory=new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("testQueue");
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                TextMessage textMessage= (TextMessage) message;
                try {
                    String text = textMessage.getText();
                    System.out.println("接收到消息"+text+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
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
        //1、创建一个连接工厂对象ConnectionFactory对象，指定服务的IP和端口号
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        //2、使用ConnectionFactory对象创建Connection连接对象
        Connection connection = connectionFactory.createConnection();
        //3、开启连接，调用start方法
        connection.start();
        //4、使用Connection对象创建session对象
        //第一个参数：是否开启事务，一般不开启，false；当第一个参数为false时，第二个参数才有意义。
        //第二个参数：消息的应答模式：手动应答、自动应答，一般使用自动应答
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        //5、使用Session创建Destination目的地对象，有两种：Queue、Topic
        Topic topic = session.createTopic("testTopic");
        //6、使用session创建一个producer对象
        MessageProducer producer = session.createProducer(topic);
        //7、使用session创建一个TextMessage对象（五种格式：StreamMessage 、MapMessage、TextMessage、ObjectMessage、BytesMessage）
        TextMessage message = session.createTextMessage("这是发布/订阅模式--生产者模式发送的消息！消息发送时间：" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        //8、使用producer对象发布消息
        producer.send(message);
        System.out.println("发布/订阅模式--生产者--发布消息成功！" + System.currentTimeMillis());
        //9、关闭资源
        producer.close();
        session.close();
        connection.close();
    }

    @Test
    public void testTopicCustomer() throws JMSException, IOException {
        //1、创建一个连接工厂对象ConnectionFactory对象，指定服务的IP和端口号
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        //2、使用ConnectionFactory对象创建Connection连接对象
        Connection connection = connectionFactory.createConnection();
        //3、开启连接，调用start方法
        connection.start();
        //4、使用Connection对象创建session对象
        //第一个参数：是否开启事务，一般不开启，false；当第一个参数为false时，第二个参数才有意义。
        //第二个参数：消息的应答模式：手动应答、自动应答，一般使用自动应答
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        //5、使用Session创建Destination目的地对象，有两种：Queue、Topic
        Topic topic = session.createTopic("testTopic");
        //6、使用session创建一个consumer对象
        MessageConsumer consumer = session.createConsumer(topic);
        //7、接受消息
        consumer.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                TextMessage textMessage = (TextMessage) message;
                try {
                    String text = textMessage.getText();
                    System.out.println("接收到的Topic模式的消息内容：" + text + ",消息接收时间：" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
        //8、等待键盘输入(程序等待，可以继续接收消息)
        //注意：Topic模式下，生产者发送消息之后再服务器上没有缓存，区别于点对点模式（可以先执行testTopicCustomer再执行testTopicProducer）
        System.in.read();
        //9、关闭资源
        consumer.close();
        session.close();
        connection.close();
    }
}

