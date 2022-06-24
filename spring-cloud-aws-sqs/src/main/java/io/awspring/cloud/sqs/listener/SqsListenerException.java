package io.awspring.cloud.sqs.listener;

import io.awspring.cloud.sqs.SqsException;

/**
 * Wrapper for exceptions thrown by the {@link MessageListenerContainer}
 * or their components.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsListenerException extends SqsException {
	public SqsListenerException(String msg) {
		super(msg);
	}

	public SqsListenerException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
