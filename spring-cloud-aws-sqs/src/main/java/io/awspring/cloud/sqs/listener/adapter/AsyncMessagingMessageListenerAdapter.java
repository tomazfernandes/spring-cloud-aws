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
		try {
			return CompletableFutures.exceptionallyCompose(invokeAsyncHandler(message),
				t -> createListenerException(message, t));
		}
		catch (Exception e) {
			return createListenerException(message, e);
		}
	}

	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> invokeAsyncHandler(Message<T> message) throws Exception {
		return (CompletableFuture<Void>) super.invokeHandler(message);
	}

	private CompletableFuture<Void> createListenerException(Message<T> message, Throwable t) {
		return CompletableFutures.failedFuture(new ListenerExecutionFailedException("Listener failed to process message "
			+ MessageHeaderUtils.getId(message), t, message));
	}

	@Override
	public CompletableFuture<Void> onMessage(Collection<Message<T>> messages) {
		try {
			return CompletableFutures.exceptionallyCompose(invokeAsyncHandler(messages),
				t -> createListenerException(messages, t));
		}
		catch (Exception e) {
			return createListenerException(messages, e);
		}
	}

	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> invokeAsyncHandler(Collection<Message<T>> messages) throws Exception {
		return (CompletableFuture<Void>) super.invokeHandler(messages);
	}

	private CompletableFuture<Void> createListenerException(Collection<Message<T>> messages, Throwable e) {
		return CompletableFutures.failedFuture(new ListenerExecutionFailedException("Listener failed to process messages "
			+ MessageHeaderUtils.getId(messages), e, messages));
	}

}
