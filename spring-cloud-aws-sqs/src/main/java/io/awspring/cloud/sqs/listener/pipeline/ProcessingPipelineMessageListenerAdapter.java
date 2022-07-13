package io.awspring.cloud.sqs.listener.pipeline;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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
