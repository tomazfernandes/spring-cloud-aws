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

import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Entrypoint for constructing a {@link MessageProcessingPipeline} {@link ComposingMessagePipelineStage}.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
class MessageProcessingPipelineBuilder<T> {

	private final Function<MessageProcessingConfiguration<T>, MessageProcessingPipeline<T>> pipelineFactory;

	public MessageProcessingPipelineBuilder(Function<MessageProcessingConfiguration<T>, MessageProcessingPipeline<T>> pipelineFactory) {
		this.pipelineFactory = pipelineFactory;
	}

	public static <T> MessageProcessingPipelineBuilder<T> first(Function<MessageProcessingConfiguration<T>, MessageProcessingPipeline<T>> pipelineFactory) {
		return new MessageProcessingPipelineBuilder<>(pipelineFactory);
	}

	public MessageProcessingPipelineBuilder<T> then(Function<MessageProcessingConfiguration<T>, MessageProcessingPipeline<T>> pipelineFactory) {
		return new MessageProcessingPipelineBuilder<>(context -> new ComposingMessagePipelineStage<>(this.pipelineFactory.apply(context), pipelineFactory.apply(context)));
	}

	public MessageProcessingPipeline<T> build(MessageProcessingConfiguration<T> context) {
		return this.pipelineFactory.apply(context);
	}

	public MessageProcessingPipelineBuilder<T> thenWrapWith(BiFunction<MessageProcessingConfiguration<T>, MessageProcessingPipeline<T>, MessageProcessingPipeline<T>> pipelineFactory) {
		return new MessageProcessingPipelineBuilder<>(context -> pipelineFactory.apply(context, this.pipelineFactory.apply(context)));
	}

	private static class ComposingMessagePipelineStage<T> implements MessageProcessingPipeline<T> {

		private final MessageProcessingPipeline<T> first;
		private final MessageProcessingPipeline<T> second;

		private ComposingMessagePipelineStage(MessageProcessingPipeline<T> first, MessageProcessingPipeline<T> second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public CompletableFuture<Message<T>> process(Message<T> input) {
			return first.process(input).thenCompose(second::process);
		}

		@Override
		public CompletableFuture<Collection<Message<T>>> process(Collection<Message<T>> input) {
			return first.process(input).thenCompose(second::process);
		}
	}

}
