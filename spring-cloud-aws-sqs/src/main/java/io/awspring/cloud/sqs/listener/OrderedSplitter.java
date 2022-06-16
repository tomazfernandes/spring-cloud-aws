package io.awspring.cloud.sqs.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class OrderedSplitter<T> implements MessageSplitter<T> {

	Logger logger = LoggerFactory.getLogger(OrderedSplitter.class);

	private final TaskExecutor taskExecutor;

	public OrderedSplitter(TaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	@Override
	public CompletableFuture<Void> splitAndProcess(Collection<Message<T>> messages, Function<Message<T>, CompletableFuture<Void>> processingPipeline) {
		logger.trace("Received messages: " + messages.stream().map(Message::getPayload).map(Object::toString).collect(Collectors.joining(", ")));
		CompletableFuture<Void> identity = new CompletableFuture<>();
		identity.complete(null);
		return messages
				.stream()
				.reduce(identity,
					(voidFuture, msg) -> voidFuture.thenCompose(theVoid -> processingPipeline.apply(msg)), (a, b) -> b);
	}
}
