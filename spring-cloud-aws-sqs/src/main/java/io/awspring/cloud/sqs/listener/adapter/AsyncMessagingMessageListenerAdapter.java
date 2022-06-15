/*
 * Copyright 2022 the original author or authors.
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

import io.awspring.cloud.messaging.support.listener.AsyncMessageListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

import java.util.concurrent.CompletableFuture;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class AsyncMessagingMessageListenerAdapter<T> extends MessagingMessageListenerAdapter implements AsyncMessageListener<T> {

	public AsyncMessagingMessageListenerAdapter(InvocableHandlerMethod handlerMethod) {
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
