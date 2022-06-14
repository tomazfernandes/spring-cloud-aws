package io.awspring.cloud.messaging.support.listener.adapter;

import io.awspring.cloud.messaging.support.listener.AsyncMessageListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

import java.util.concurrent.CompletableFuture;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class AsyncSingleMessageListenerAdapter<T> extends MessagingMessageListenerAdapter implements AsyncMessageListener<T> {

	public AsyncSingleMessageListenerAdapter(InvocableHandlerMethod handlerMethod) {
		super(handlerMethod);
	}

	@Override
	public CompletableFuture<Void> onMessage(Message<T> message) {
		try {
			Object result = super.invokeHandler(message);
			return result instanceof CompletableFuture
				? ((CompletableFuture<?>) result).thenRun(() -> {})
				: CompletableFuture.completedFuture(null);
		} catch (Exception e) {
			CompletableFuture<Void> future = new CompletableFuture<>();
			future.completeExceptionally(e);
			return future;
		}
	}
}
