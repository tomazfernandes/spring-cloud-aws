package io.awspring.cloud.sqs.listener.pipeline;

import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.BackPressureHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class BackPressureReleaseStage<T> implements MessageProcessingPipeline<T> {

	private static final Logger logger = LoggerFactory.getLogger(BackPressureReleaseStage.class);

	private final BackPressureHandler backPressureHandler;

	public BackPressureReleaseStage(MessageProcessingContext<T> context) {
		this.backPressureHandler = context.getBackPressureHandler();
	}

	@Override
	public CompletableFuture<Message<T>> process(Message<T> message) {
		logger.debug("Releasing sempahore for message {}", MessageHeaderUtils.getId(message));
		this.backPressureHandler.release(1);
		return CompletableFuture.completedFuture(message);
	}

	@Override
	public CompletableFuture<Collection<Message<T>>> process(Collection<Message<T>> messages) {
		logger.debug("Releasing {} semaphores", messages.size());
		this.backPressureHandler.release(messages.size());
		return CompletableFuture.completedFuture(messages);
	}
}
