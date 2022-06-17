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
public class FanOutSplitter<T> extends AbstractMessageSplitter<T> {

	Logger logger = LoggerFactory.getLogger(FanOutSplitter.class);

	@Override
	protected CompletableFuture<Void> doSplitAndProcess(Collection<Message<T>> messages,
														Function<Message<T>, CompletableFuture<Void>> processingPipeline) {

		return CompletableFuture
			.allOf(messages.stream()
				.map(msg -> CompletableFuture.supplyAsync(() -> processingPipeline.apply(msg), super.getTaskExecutor())
						.thenCompose(x -> x))
				.toArray(CompletableFuture[]::new));
	}
}
