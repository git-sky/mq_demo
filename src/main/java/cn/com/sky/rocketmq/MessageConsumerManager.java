package cn.com.sky.rocketmq;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.com.sky.rocketmq.utils.RocketMQUtil;
import cn.com.sky.rocketmq.utils.TopicInfo;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 */
public class MessageConsumerManager {

	private static final Logger logger = LoggerFactory.getLogger(MessageConsumerManager.class);
	private static DefaultMQPushConsumer pushConsumer;
	private static DefaultMQPullConsumer pullConsumer;
	private static TopicInfo topicInfo = new TopicInfo();

	public static MQConsumer getPushConsumer(MessageListener listener) throws MQClientException {
		pushConsumer = RocketMQUtil.createConsumerPush(topicInfo.getGroupName(), topicInfo.getNamesrvAddr(), topicInfo.getInstanceName());
		pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		pushConsumer.subscribe(topicInfo.getTopic(), null);
		pushConsumer.setMessageModel(MessageModel.CLUSTERING);
		pushConsumer.registerMessageListener(listener);
		return pushConsumer;

	}

	public static MQConsumer getPullConsumer(MessageListener listener) throws MQClientException {
		pullConsumer = RocketMQUtil.createConsumerPull(topicInfo.getGroupName(), topicInfo.getNamesrvAddr(), topicInfo.getInstanceName());
		pullConsumer.setMessageModel(MessageModel.CLUSTERING);
		Set<String> set = new HashSet<String>();
		set.add(topicInfo.getTopic());
		pullConsumer.setRegisterTopics(set);
		return pullConsumer;
	}

}
