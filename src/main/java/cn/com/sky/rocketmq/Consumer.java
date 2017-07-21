package cn.com.sky.rocketmq;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Consumer {

	public static void main(String[] args) {

	}

	public void start() throws MQClientException {
		MessageListener ml = buildMessageListener(false);// 并行
		DefaultMQPushConsumer consumer = (DefaultMQPushConsumer) MessageConsumerManager.getPushConsumer(ml);
		consumer.start();

	}

	/**
	 * 构建消息监听器
	 *
	 * @param isOrdered
	 *            是否顺序监听
	 * @return
	 */
	public MessageListener buildMessageListener(boolean isOrdered) {
		// 默认并行
		if (!isOrdered) {
			MessageListener listener = new MessageListenerConcurrently() {

				@Override
				public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
					return null;
				}
			};
			return listener;
		}
		return null;

	}
}
