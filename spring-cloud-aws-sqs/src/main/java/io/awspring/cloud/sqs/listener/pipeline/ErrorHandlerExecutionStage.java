package io.awspring.cloud.sqs.listener.pipeline;

import io.awspring.cloud.sqs.CompletableFutures;
import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
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
public class ErrorHandlerExecutionStage<T> implements MessageProcessingPipeline<T> {

	private static final Logger logger = LoggerFactory.getLogger(ErrorHandlerExecutionStage.class);

	private final AsyncErrorHandler<T> errorHandler;

	private final MessageProcessingPipeline<T> wrapped;

	public ErrorHandlerExecutionStage(MessageProcessingContext<T> context, MessageProcessingPipeline<T> wrapped) {
		this.errorHandler = context.getErrorHandler();
		this.wrapped = wrapped;
	}

	@Override
	public CompletableFuture<Message<T>> process(Message<T> message) {
		logger.debug("Processing message {}", MessageHeaderUtils.getId(message));
		return CompletableFutures.exceptionallyCompose(wrapped.process(message),
			t -> errorHandler.handleError(message, t)
				.whenComplete((v, t2) -> {
					if (t2 != null) {
						logger.error("Error processing message {}", MessageHeaderUtils.getId(message), t2);
					} else {
						logger.debug("Processed message {}", MessageHeaderUtils.getId(message));
					}
				})
				.thenApply(theVoid -> message));
	}

	@Override
	public CompletableFuture<Collection<Message<T>>> process(Collection<Message<T>> messages) {
		logger.debug("Processing {} messages", messages.size());
		return CompletableFutures.exceptionallyCompose(wrapped.process(messages),
			t -> errorHandler.handleError(messages, t).thenApply(theVoid -> messages));
	}
}
