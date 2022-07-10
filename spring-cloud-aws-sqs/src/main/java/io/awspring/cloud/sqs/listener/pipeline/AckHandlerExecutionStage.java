package io.awspring.cloud.sqs.listener.pipeline;

import io.awspring.cloud.sqs.CompletableFutures;
import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.acknowledgement.AckHandler;
import io.awspring.cloud.sqs.listener.interceptor.MessageVisibilityExtenderInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class AckHandlerExecutionStage<T> implements MessageProcessingPipeline<T> {

	private static final Logger logger = LoggerFactory.getLogger(AckHandlerExecutionStage.class);

	private final MessageProcessingPipeline<T> wrapped;

	private final AckHandler<T> ackHandler;

	public AckHandlerExecutionStage(MessageProcessingContext<T> context, MessageProcessingPipeline<T> wrapped) {
		this.wrapped = wrapped;
		this.ackHandler = context.getAckHandler();
	}

	@Override
	public CompletableFuture<Message<T>> process(Message<T> message) {
		logger.debug("Processing message {}", MessageHeaderUtils.getId(message));
		return CompletableFutures.exceptionallyCompose(this.wrapped.process(message)
			.thenCompose(ackHandler::onSuccess), t -> ackHandler.onError(message, t)).thenApply(theVoid -> message);
	}

	@Override
	public CompletableFuture<Collection<Message<T>>> process(Collection<Message<T>> messages) {
		logger.debug("Processing {} messages", messages.size());
		return CompletableFutures.exceptionallyCompose(this.wrapped.process(messages)
			.thenCompose(ackHandler::onSuccess), t -> ackHandler.onError(messages, t)).thenApply(theVoid -> messages);
	}

}
