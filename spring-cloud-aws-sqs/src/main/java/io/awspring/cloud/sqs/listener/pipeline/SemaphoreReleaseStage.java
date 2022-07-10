package io.awspring.cloud.sqs.listener.pipeline;

import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.interceptor.MessageVisibilityExtenderInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SemaphoreReleaseStage<T> implements MessageProcessingPipeline<T> {

	private static final Logger logger = LoggerFactory.getLogger(SemaphoreReleaseStage.class);

	private final Semaphore semaphore;

	public SemaphoreReleaseStage(MessageProcessingContext<T> context) {
		this.semaphore = context.getSemaphore();
	}

	@Override
	public CompletableFuture<Message<T>> process(Message<T> message) {
		logger.debug("Releasing sempahore for message {}", MessageHeaderUtils.getId(message));
		this.semaphore.release();
		return CompletableFuture.completedFuture(message);
	}

	@Override
	public CompletableFuture<Collection<Message<T>>> process(Collection<Message<T>> messages) {
		logger.debug("Releasing {} semaphores", messages.size());
		this.semaphore.release(messages.size());
		return CompletableFuture.completedFuture(messages);
	}
}
