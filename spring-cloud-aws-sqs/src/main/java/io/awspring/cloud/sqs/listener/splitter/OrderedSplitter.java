package io.awspring.cloud.sqs.listener.splitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class OrderedSplitter<T> extends AbstractMessageSplitter<T> {

	Logger logger = LoggerFactory.getLogger(OrderedSplitter.class);

	@Override
	protected Collection<CompletableFuture<Void>> doSplitAndProcessMessages(Collection<Message<T>> messages, Function<Message<T>, CompletableFuture<Void>> processingPipeline) {
		logger.debug("Splitting {} messages", messages.size());
		CompletableFuture<Void> identity = new CompletableFuture<>();
		identity.complete(null);
		CompletableFuture.supplyAsync(() -> messages
				.stream()
				.reduce(identity,
					(voidFuture, msg) -> voidFuture.thenCompose(theVoid -> processingPipeline.apply(msg)), (a, b) -> b), getTaskExecutor())
			.join();
		return super.returnCompletedVoidFutures(messages);
	}
}
