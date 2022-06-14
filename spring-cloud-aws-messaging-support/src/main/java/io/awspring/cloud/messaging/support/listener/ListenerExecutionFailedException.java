package io.awspring.cloud.messaging.support.listener;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class ListenerExecutionFailedException extends RuntimeException {

	public ListenerExecutionFailedException(String message, Exception ex) {
		super(message, ex);
	}

}

