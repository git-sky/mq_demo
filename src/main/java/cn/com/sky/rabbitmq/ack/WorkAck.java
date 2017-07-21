package cn.com.sky.rabbitmq.ack;

import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * 显式ack
 * 
 * 当某个工作者（接收者）被杀死时，我们希望将任务传递给另一个工作者。
 * 
 * 为了保证消息永远不会丢失，RabbitMQ支持消息应答（message acknowledgments）。
 * 
 * 消费者发送应答给RabbitMQ，告诉它信息已经被接收和处理，然后RabbitMQ可以自由的进行信息删除。
 * 
 * 如果消费者被杀死而没有发送应答，RabbitMQ会认为该信息没有被完全的处理，然后将会重新转发给别的消费者。
 * 
 * 通过这种方式，你可以确认信息不会被丢失，即使消者偶尔被杀死。
 * 
 * 这种机制并没有超时时间这么一说，RabbitMQ只有在消费者连接断开是重新转发此信息。如果消费者处理一个信息需要耗费特别特别长的时间是允许的。
 */
public class WorkAck {
	// 队列名称
	private final static String QUEUE_NAME = "workqueue";

	public static void main(String[] argv) throws java.io.IOException, java.lang.InterruptedException, TimeoutException {
		// 区分不同工作进程的输出
		int hashCode = Work.class.hashCode();
		// 创建连接和频道
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		// 声明队列
		channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		System.out.println(hashCode + " [*] Waiting for messages. To exit press CTRL+C");
		QueueingConsumer consumer = new QueueingConsumer(channel);
		// 指定消费队列
		boolean ack = false; // 打开应答机制
		channel.basicConsume(QUEUE_NAME, ack, consumer);
		while (true) {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			String message = new String(delivery.getBody());

			System.out.println(hashCode + " [x] Received '" + message + "'");
			doWork(message);
			System.out.println(hashCode + " [x] Done");
			// 显式发送应答
			channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

		}

	}

	/**
	 * 每个点耗时1s
	 * 
	 * @param task
	 * @throws InterruptedException
	 */
	private static void doWork(String task) throws InterruptedException {
		for (char ch : task.toCharArray()) {
			if (ch == '.')
				Thread.sleep(1000);
		}
	}
}