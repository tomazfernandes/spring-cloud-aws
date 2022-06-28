package io.awspring.cloud.sqs.listener.errorhandler;

import org.springframework.messaging.Message;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface ErrorHandler<T> {

	/**
	 * Handle errors thrown when processing a {@link Message}.
	 * processing the given {@link Message}.
	 * @param message the message.
	 * @param t the thrown exception.
	 */
	void handle(Message<T> message, Throwable t);

}
