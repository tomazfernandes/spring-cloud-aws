package io.awspring.cloud.sqs.listener;

import org.springframework.messaging.Message;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface MessageListener<T> {

	void onMessage(Message<T> message);

}
