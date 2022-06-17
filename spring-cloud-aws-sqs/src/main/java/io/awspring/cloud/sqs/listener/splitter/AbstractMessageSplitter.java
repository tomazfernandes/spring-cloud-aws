package io.awspring.cloud.sqs.listener.splitter;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
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
public abstract class AbstractMessageSplitter<T> implements AsyncMessageSplitter<T>, SmartLifecycle {

	private static final int DEFAULT_CORE_SIZE = 10;

	private volatile boolean isRunning;

	private int coreSize = DEFAULT_CORE_SIZE;

	private TaskExecutor taskExecutor;

	private final Object lifecycleMonitor = new Object();

	public void setCoreSize(int coreSize) {
		this.coreSize = coreSize;
	}

	protected TaskExecutor getTaskExecutor() {
		return this.taskExecutor;
	}

	@Override
	public void start() {
		synchronized (this.lifecycleMonitor) {
			if (this.isRunning) {
				return;
			}
			this.isRunning = true;
			this.taskExecutor = createTaskExecutor();
		}
	}

	public CompletableFuture<Void> splitAndProcess(Collection<Message<T>> messages,
												  Function<Message<T>, CompletableFuture<Void>> processingPipeline) {
		if (!this.isRunning) {
			return CompletableFuture.completedFuture(null);
		}
		return doSplitAndProcess(messages, processingPipeline);
	}

	protected abstract CompletableFuture<Void> doSplitAndProcess(Collection<Message<T>> messages,
																Function<Message<T>, CompletableFuture<Void>> processingPipeline);

	@Override
	public void stop() {
		synchronized (this.lifecycleMonitor) {
			if (!this.isRunning) {
				return;
			}
			this.isRunning = false;
			if (this.taskExecutor instanceof DisposableBean) {
				try {
					((DisposableBean) this.taskExecutor).destroy();
				} catch (Exception e) {
					throw new IllegalStateException("Error shutting down executor", e);
				}
			}
		}
	}

	@Override
	public boolean isRunning() {
		return this.isRunning;
	}

	protected TaskExecutor createTaskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.setMaxPoolSize(this.coreSize);
		taskExecutor.setCorePoolSize(this.coreSize);
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

}
