package io.awspring.cloud.sqs.listener.pipeline;

import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
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
public class InterceptorExecutionStage<T> implements MessageProcessingPipeline<T> {

	private static final Logger logger = LoggerFactory.getLogger(InterceptorExecutionStage.class);

	private final Collection<AsyncMessageInterceptor<T>> messageInterceptors;

	public InterceptorExecutionStage(MessageProcessingContext<T> context) {
		messageInterceptors = context.getMessageInterceptors();
	}

	@Override
	public CompletableFuture<Message<T>> process(Message<T> message) {
		logger.debug("Processing message {}", MessageHeaderUtils.getId(message));
		return this.messageInterceptors.stream().reduce(CompletableFuture.completedFuture(message),
			(messageFuture, interceptor) -> messageFuture.thenCompose(interceptor::intercept), (a, b) -> a)
			.whenComplete((v, t) -> {
				if (t != null) {
					logger.error("Error processing message {}", MessageHeaderUtils.getId(message), t);
				} else {
					logger.debug("Processed message {}", MessageHeaderUtils.getId(message));
				}
			})
			;
	}

	@Override
	public CompletableFuture<Collection<Message<T>>> process(Collection<Message<T>> messages) {
		logger.debug("Processing {} messages", messages.size());
		return this.messageInterceptors.stream().reduce(CompletableFuture.completedFuture(messages),
			(messageFuture, interceptor) -> messageFuture.thenCompose(interceptor::intercept), (a, b) -> a);
	}
}
