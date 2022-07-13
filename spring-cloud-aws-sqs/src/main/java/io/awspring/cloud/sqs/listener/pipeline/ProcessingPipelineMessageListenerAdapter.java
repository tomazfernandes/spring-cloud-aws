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

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * {@link AsyncMessageListener} adapter for receiving an `onMessage` call and invoking
 * the {@link MessageProcessingPipeline} with the payload.
 *
 * @param <T> the {@link Message} type.
 */
public class ProcessingPipelineMessageListenerAdapter<T> implements AsyncMessageListener<T> {

	private final MessageProcessingPipeline<T> pipeline;

	private ProcessingPipelineMessageListenerAdapter(MessageProcessingConfiguration<T> context) {
		this.pipeline = MessageProcessingPipelineBuilder
			.<T>first(InterceptorExecutionStage::new)
			.then(MessageListenerExecutionStage::new)
			.thenWrapWith(ErrorHandlerExecutionStage::new)
			.thenWrapWith(AckHandlerExecutionStage::new)
			.build(context);
	}

	public static <T> ProcessingPipelineMessageListenerAdapter<T> create(Function<MessageProcessingConfiguration.Builder<T>,
			MessageProcessingConfiguration.Builder<T>> contextConsumer) {
		return new ProcessingPipelineMessageListenerAdapter<>(contextConsumer.apply(MessageProcessingConfiguration.builder()).build());
	}

	@Override
	public CompletableFuture<Void> onMessage(Message<T> message) {
		return this.pipeline.process(message).thenRun(() -> {
		});
	}

	@Override
	public CompletableFuture<Void> onMessage(Collection<Message<T>> messages) {
		return this.pipeline.process(messages).thenRun(() -> {
		});
	}
}
