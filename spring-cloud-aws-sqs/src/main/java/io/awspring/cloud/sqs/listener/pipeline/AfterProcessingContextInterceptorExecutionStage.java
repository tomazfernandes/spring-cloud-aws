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
package io.awspring.cloud.sqs.listener.pipeline;

import io.awspring.cloud.sqs.CompletableFutures;
import io.awspring.cloud.sqs.listener.ListenerExecutionFailedException;
import io.awspring.cloud.sqs.listener.MessageProcessingContext;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Stage responsible for executing the {@link AsyncMessageInterceptor}s.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class AfterProcessingContextInterceptorExecutionStage<T> implements MessageProcessingPipeline<T> {

	private static final Logger logger = LoggerFactory.getLogger(AfterProcessingContextInterceptorExecutionStage.class);

	public AfterProcessingContextInterceptorExecutionStage(MessageProcessingConfiguration<T> configuration) {
	}

	@Override
	public CompletableFuture<Message<T>> process(CompletableFuture<Message<T>> messageFuture, MessageProcessingContext<T> context) {
		return CompletableFutures.handleCompose(messageFuture,
				(v, t) -> t == null
					? applyInterceptors(v, null, context.getInterceptors())
					: applyInterceptors(ListenerExecutionFailedException.unwrapMessage(t), t, context.getInterceptors())
						.thenCompose(msg -> CompletableFutures.failedFuture(t)));
	}

	private CompletableFuture<Message<T>> applyInterceptors(Message<T> message, Throwable t, List<AsyncMessageInterceptor<T>> messageInterceptors) {
		return messageInterceptors.stream()
			.reduce(CompletableFuture.<Void>completedFuture(null),
				(voidFuture, interceptor) -> voidFuture.thenCompose(theVoid -> interceptor.afterProcessing(message, t)), (a, b) -> a)
			.thenApply(theVoid -> message);
	}

	@Override
	public CompletableFuture<Collection<Message<T>>> processMany(CompletableFuture<Collection<Message<T>>> messagesFuture, MessageProcessingContext<T> context) {
		return CompletableFutures.handleCompose(messagesFuture,
			(v, t) -> t == null
				? applyInterceptors(v, null, context.getInterceptors())
				: applyInterceptors(ListenerExecutionFailedException.unwrapMessages(t), t, context.getInterceptors())
					.thenCompose(msg -> CompletableFutures.failedFuture(t)));
	}

	private CompletableFuture<Collection<Message<T>>> applyInterceptors(Collection<Message<T>> messages, Throwable t, List<AsyncMessageInterceptor<T>> messageInterceptors) {
		return messageInterceptors.stream()
			.reduce(CompletableFuture.<Void>completedFuture(null),
				(voidFuture, interceptor) -> voidFuture.thenCompose(theVoid -> interceptor.afterProcessing(messages, t)), (a, b) -> a)
			.thenApply(theVoid -> messages);
	}

}
