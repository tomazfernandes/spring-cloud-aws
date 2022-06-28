package io.awspring.cloud.sqs.listener.adapter;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import java.util.concurrent.CompletableFuture;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class DelegatingMessageListenerAdapter<T> implements AsyncMessageListener<T> {

	private final AsyncMessageListener<T> delegate;

	public DelegatingMessageListenerAdapter(AsyncMessageListener<T> delegate) {
		Assert.notNull(delegate, "delegate cannot be null");
		this.delegate = delegate;
	}

	@Override
	public CompletableFuture<Void> onMessage(Message<T> message) {
		return delegate.onMessage(message);
	}

	protected AsyncMessageListener<T> getDelegate() {
		return this.delegate;
	}
}
