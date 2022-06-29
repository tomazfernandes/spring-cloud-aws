/*
 * Copyright 2013-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.awspring.cloud.sqs.listener.adapter;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import java.util.concurrent.CompletableFuture;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * {@link AsyncMessageListener} implementation that delegates {@link #onMessage} calls.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class DelegatingMessageListenerAdapter<T> implements AsyncMessageListener<T> {

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
