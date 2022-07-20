package io.awspring.cloud.sqs.listener;


import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Representation of a processing context that can be used for communication
 * between components.
 * This class is immutable and thread-safe.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class MessageProcessingContext<T> {

	private final List<AsyncMessageInterceptor<T>> contextInterceptors;

	public MessageProcessingContext(List<AsyncMessageInterceptor<T>> contextInterceptors) {
		this.contextInterceptors = Collections.unmodifiableList(contextInterceptors);
	}

	public List<AsyncMessageInterceptor<T>> getContextInterceptors() {
		return this.contextInterceptors;
	}

	public MessageProcessingContext<T> addInterceptor(AsyncMessageInterceptor<T> interceptor) {
		List<AsyncMessageInterceptor<T>> interceptors = new ArrayList<>(this.contextInterceptors);
		interceptors.add(interceptor);
		return new MessageProcessingContext<>(interceptors);
	}
}
