package cn.com.sky.rocketmq.demo2;

import java.util.Date;

import cn.com.sky.rocketmq.utils.Constants;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {
	public static void main(String[] args) throws MQClientException, InterruptedException {
		DefaultMQProducer producer = new DefaultMQProducer(Constants.GROUP_NAME);
		producer.setNamesrvAddr(Constants.NAME_SERVER_ADDR);
		producer.setInstanceName(Constants.INSTATNCE);
		producer.start();
		try {
			for (int i = 0; i < 3; i++) {
				Message msg = new Message(Constants.TOPIC,// topic
						Constants.TAG,// tag
						(new Date() + "Hello RocketMQ ,QuickStart" + i).getBytes()// body
				);
				SendResult sendResult = producer.send(msg);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		producer.shutdown();
	}
}