package io.awspring.cloud.sqs.listener.pipeline;

import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class MessageProcessingPipelineBuilder<T> {

	private final Function<MessageProcessingContext<T>, MessageProcessingPipeline<T>> pipelineFactory;

	public MessageProcessingPipelineBuilder(Function<MessageProcessingContext<T>, MessageProcessingPipeline<T>> pipelineFactory) {
		this.pipelineFactory = pipelineFactory;
	}

	public static <T> MessageProcessingPipelineBuilder<T> first(Function<MessageProcessingContext<T>, MessageProcessingPipeline<T>> pipelineFactory) {
		return new MessageProcessingPipelineBuilder<>(pipelineFactory);
	}

	public MessageProcessingPipelineBuilder<T> then(Function<MessageProcessingContext<T>, MessageProcessingPipeline<T>> pipelineFactory) {
		return new MessageProcessingPipelineBuilder<>(context -> new ComposingMessagePipelineStage<>(this.pipelineFactory.apply(context), pipelineFactory.apply(context)));
	}

	public MessageProcessingPipeline<T> build(MessageProcessingContext<T> context) {
		return this.pipelineFactory.apply(context);
	}

	public MessageProcessingPipelineBuilder<T> wrappedWith(BiFunction<MessageProcessingContext<T>, MessageProcessingPipeline<T>, MessageProcessingPipeline<T>> pipelineFactory) {
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
