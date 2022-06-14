package io.awspring.cloud.messaging.support.config;

import io.awspring.cloud.messaging.support.listener.AsyncMessageListener;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface MessageListenerFactory<T> {

	AsyncMessageListener<T> createFor(Endpoint<T> endpoint);

}
