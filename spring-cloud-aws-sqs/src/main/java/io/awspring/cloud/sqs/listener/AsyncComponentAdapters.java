package io.awspring.cloud.sqs.listener;


import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.errorhandler.ErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.interceptor.MessageInterceptor;


/**
 * Utility class for adapting blocking components to asynchronous
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class AsyncComponentAdapters {

	private AsyncComponentAdapters() {}

	/**
	 * Adapt the provided {@link ErrorHandler} to an {@link AsyncErrorHandler}
	 * @param errorHandler the handler to be adapted
	 * @param <T> the message payload type
	 * @return the adapted component.
	 */
	public static <T> AsyncErrorHandler<T> adapt(ErrorHandler<T> errorHandler) {
		return (message, t) -> AsyncExecutionAdapters.adaptFromBlocking(() -> errorHandler.handle(message, t));
	}

	/**
	 * Adapt the provided {@link MessageInterceptor} to an {@link AsyncMessageInterceptor}
	 * @param messageInterceptor the interceptor to be adapted
	 * @param <T> the message payload type
	 * @return the adapted component.
	 */
	public static <T> AsyncMessageInterceptor<T> adapt(MessageInterceptor<T> messageInterceptor) {
		return msg -> AsyncExecutionAdapters.adaptFromBlocking(() -> messageInterceptor.intercept(msg));
	}

	/**
	 * Adapt the provided {@link MessageListener} to an {@link AsyncMessageListener}
	 * @param messageListener the listener to be adapted
	 * @param <T> the message payload type
	 * @return the adapted component.
	 */
	public static <T> AsyncMessageListener<T> adapt(MessageListener<T> messageListener) {
		return msg -> AsyncExecutionAdapters.adaptFromBlocking(() -> messageListener.onMessage(msg));
	}

}
