package io.awspring.cloud.sqs.listener.interceptor;

import org.springframework.messaging.Message;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
@FunctionalInterface
public interface MessageInterceptor<T> {

	/**
	 * Intercept the message before processing.
	 * @param message the message to be intercepted.
	 */
	Message<T> intercept(Message<T> message);

}
