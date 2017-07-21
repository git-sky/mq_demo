package cn.com.sky.rocketmq.utils;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;

public class RocketMQUtil {

	public static DefaultMQProducer createProducer(String groupName, String url, String instanceName) {
		DefaultMQProducer producer = new DefaultMQProducer(groupName);
		producer.setNamesrvAddr(url);
		producer.setInstanceName(instanceName);
		return producer;
	}

	public static DefaultMQPushConsumer createConsumerPush(String groupName, String namesrvAddr, String instanceName) {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
		consumer.setNamesrvAddr(namesrvAddr);
		consumer.setInstanceName(instanceName);
		return consumer;
	}

	public static DefaultMQPullConsumer createConsumerPull(String groupName, String namesrvAddr, String instanceName) {
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(groupName);
		consumer.setNamesrvAddr(namesrvAddr);
		consumer.setInstanceName(instanceName);
		return consumer;
	}

	public static void shutdownHook(final MQProducer producer) {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				if (producer != null) {
					producer.shutdown();
				}
			}
		}));
	}

	public static void shutdownHook(final MQPushConsumer consumer) {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				if (consumer != null) {
					consumer.shutdown();
				}
			}
		}));
	}

	public static void shutdownHook(final MQPullConsumer consumer) {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				if (consumer != null) {
					consumer.shutdown();
				}
			}
		}));
	}

}
