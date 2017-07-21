package cn.com.sky.rocketmq.demo1;

import cn.com.sky.rocketmq.utils.Constants;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * 
 */
public class Producer {
	public static void main(String[] args) {

		DefaultMQProducer producer = new DefaultMQProducer(Constants.GROUP_NAME);
		producer.setNamesrvAddr(Constants.NAME_SERVER_ADDR);
		try {
			producer.start();

			SendResult result = producer.send(produceMessage());

			System.out.println(result.getMsgId() + result.getSendStatus());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.shutdown();
		}
	}

	private static Message produceMessage() {
		return new Message(Constants.TOPIC, Constants.TAG, "1", "Just for test.".getBytes());
	}
}
