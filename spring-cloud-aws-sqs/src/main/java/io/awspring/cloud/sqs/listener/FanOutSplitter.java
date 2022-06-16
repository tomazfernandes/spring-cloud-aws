package io.awspring.cloud.sqs.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class FanOutSplitter<T> implements MessageSplitter<T> {

	Logger logger = LoggerFactory.getLogger(FanOutSplitter.class);

	private final TaskExecutor taskExecutor;

	public FanOutSplitter(TaskExecutor taskExecutor) {
		ThreadPoolTaskExecutor taskExecutor1 = new ThreadPoolTaskExecutor();
		taskExecutor1.setMaxPoolSize(10);
		taskExecutor1.setCorePoolSize(10);
		taskExecutor1.afterPropertiesSet();
		this.taskExecutor = taskExecutor1;
	}

	@Override
	public CompletableFuture<Void> splitAndProcess(Collection<Message<T>> messages, Function<Message<T>, CompletableFuture<Void>> processingPipeline) {
		return CompletableFuture
			.allOf(messages.stream()
				.map(msg -> CompletableFuture.supplyAsync(() -> processingPipeline.apply(msg), this.taskExecutor)
						.thenCompose(x -> x))
				.toArray(CompletableFuture[]::new));
	}
}
