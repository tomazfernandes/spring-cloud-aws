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

import io.awspring.cloud.sqs.CompletableFutures;
import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import io.awspring.cloud.sqs.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * {@link AsyncMessageListener} implementation to handle a message by invoking a method handler.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class AsyncMessagingMessageListenerAdapter<T> extends AbstractMethodInvokingListenerAdapter<T>
		implements AsyncMessageListener<T> {

	public AsyncMessagingMessageListenerAdapter(InvocableHandlerMethod handlerMethod) {
		super(handlerMethod);
	}

	@Override
	public CompletableFuture<Void> onMessage(Message<T> message) {
		return CompletableFutures.exceptionallyCompose(invokeAsyncHandler(message),
				t -> CompletableFutures.failedFuture(createListenerException(message, t)));
	}

	@Override
	public CompletableFuture<Void> onMessage(Collection<Message<T>> messages) {
		return CompletableFutures.exceptionallyCompose(invokeAsyncHandler(messages),
			t -> CompletableFutures.failedFuture(createListenerException(messages, t)));
	}

	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> invokeAsyncHandler(Collection<Message<T>> messages){
		return (CompletableFuture<Void>) super.invokeHandler(messages);
	}

	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> invokeAsyncHandler(Message<T> message) {
		return (CompletableFuture<Void>) super.invokeHandler(message);
	}
}
