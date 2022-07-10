package io.awspring.cloud.sqs.listener.pipeline;

import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.AsyncMessageListener;
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
public class MessageListenerExecutionStage<T> implements MessageProcessingPipeline<T> {

	private static final Logger logger = LoggerFactory.getLogger(MessageListenerExecutionStage.class);

	private final AsyncMessageListener<T> messageListener;

	public MessageListenerExecutionStage(MessageProcessingContext<T> context) {
		this.messageListener = context.getMessageListener();
	}

	@Override
	public CompletableFuture<Message<T>> process(Message<T> message) {
		logger.debug("Processing message {}", MessageHeaderUtils.getId(message));
		return this.messageListener.onMessage(message)
			.whenComplete((v, t) -> {
				if (t != null) {
					logger.error("Error processing message {}", MessageHeaderUtils.getId(message), t);
				} else {
					logger.debug("Processed message {}", MessageHeaderUtils.getId(message));
				}
			})
			.thenApply(theVoid -> message);
	}

	@Override
	public CompletableFuture<Collection<Message<T>>> process(Collection<Message<T>> messages) {
		logger.debug("Processing {} messages", messages.size());
		return this.messageListener.onMessage(messages).thenApply(theVoid -> messages);
	}
}
