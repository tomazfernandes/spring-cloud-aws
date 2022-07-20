package io.awspring.cloud.sqs.listener.source;


import io.awspring.cloud.sqs.listener.BackPressureHandler;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.sink.MessageProcessingContext;
import io.awspring.cloud.sqs.listener.sink.MessageProcessingResult;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Base {@link PollingMessageSource} implementation with
 * {@link org.springframework.context.SmartLifecycle} and backpressure handling capabilities.
 *
 * The connected {@link MessageSink} should use the provided completion callback to signal
 * each completed message processing.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractPollingMessageSource<T> implements PollingMessageSource<T> {

	private static final Logger logger = LoggerFactory.getLogger(AbstractPollingMessageSource.class);

	private String pollingEndpointName;

	private int messagesPerPoll;

	private Duration pollTimeout;

	private TaskExecutor taskExecutor;

	private BackPressureHandler backPressureHandler;

	private Duration shutdownTimeout;

	private MessageSink<T> messageSink;

	private volatile boolean running;

	private final Object lifecycleMonitor = new Object();

	private final Collection<CompletableFuture<?>> pollingFutures = Collections.synchronizedCollection(new ArrayList<>());

	@Override
	public void configure(ContainerOptions containerOptions) {
		this.messagesPerPoll = containerOptions.getMessagesPerPoll();
		this.pollTimeout = containerOptions.getPollTimeout();
		this.shutdownTimeout = containerOptions.getShutDownTimeout();
	}

	@Override
	public void setPollingEndpointName(String pollingEndpointName) {
		Assert.isTrue(StringUtils.hasText(pollingEndpointName), "pollingEndpointName must have text");
		this.pollingEndpointName = pollingEndpointName;
	}

	@Override
	public void setBackPressureHandler(BackPressureHandler backPressureHandler) {
		Assert.notNull(backPressureHandler, "backPressureHandler cannot be null");
		this.backPressureHandler = backPressureHandler;
	}

	@Override
	public void setMessageSink(MessageSink<T> messageSink) {
		this.messageSink = messageSink;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	@Override
	public void start() {
		if (isRunning()) {
			logger.debug("Message source for queue {} already running.", this.pollingEndpointName);
			return;
		}
		synchronized (this.lifecycleMonitor) {
			Assert.notNull(this.messageSink, "No MessageSink was set");
			logger.debug("Starting MessageSource for queue {}", this.pollingEndpointName);
			this.running = true;
			doStart();
			this.taskExecutor = createTaskExecutor();
			startPollingThread();
		}
	}

	protected void doStart() {
	}

	private AsyncListenableTaskExecutor createTaskExecutor() {
		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
		executor.setThreadNamePrefix(this.pollingEndpointName + "-");
		return executor;
	}

	private void startPollingThread() {
		this.taskExecutor.execute(this::pollAndEmitMessages);
	}

	private void pollAndEmitMessages() {
		while (isRunning()) {
			try {
				if (!isRunning()) {
					continue;
				}
				logger.debug("Requesting {} permits for queue {}", this.messagesPerPoll, this.pollingEndpointName);
				int permits = this.backPressureHandler.request(this.messagesPerPoll);
				if (permits == 0) {
					logger.debug("No permits acquired.");
					continue;
				}
				logger.debug("{} permits acquired for queue {}", permits, this.pollingEndpointName);
				if (!isRunning()) {
					logger.debug("MessageSource was stopped after permits where acquired. Returning {} permits.", permits);
					this.backPressureHandler.release(permits);
					continue;
				}
				managePollingFuture(doPollForMessages())
					.exceptionally(this::handlePollingException)
					.thenApply(this::releaseUnusedPermits)
					.thenCompose(this::emitMessages)
					.exceptionally(this::handleSinkException);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("MessageSource thread interrupted for endpoint " + this.pollingEndpointName, e);
			}
			catch (Exception e) {
				logger.error("Error in MessageSource for queue {}. Resuming.", this.pollingEndpointName, e);
			}
		}
		logger.debug("Execution thread stopped for queue {}.", this.pollingEndpointName);
	}

	protected abstract CompletableFuture<Collection<Message<T>>> doPollForMessages();

	public Collection<Message<T>> releaseUnusedPermits(Collection<Message<T>> msgs) {
		int permitsToRelease = this.messagesPerPoll - msgs.size();
		logger.debug("Releasing {} unused permits for queue {}", permitsToRelease, this.pollingEndpointName);
		this.backPressureHandler.release(permitsToRelease);
		return msgs;
	}

	private CompletableFuture<Void> emitMessages(Collection<Message<T>> messages) {
		if (messages.isEmpty()) {
			return CompletableFuture.completedFuture(null);
		}
		return this.messageSink.emit(messages, MessageProcessingContext
			.withCompletionCallback(msg -> this.backPressureHandler.release(1)));
	}

	private Void handleSinkException(Throwable throwable) {
		logger.error("Sink returned an error.", throwable);
		return null;
	}

	private Collection<Message<T>> handlePollingException(Throwable t) {
		logger.error("Error polling for messages.", t);
		return Collections.emptyList();
	}

	private <F> CompletableFuture<F> managePollingFuture(CompletableFuture<F> pollingFuture) {
		this.pollingFutures.add(pollingFuture);
		pollingFuture.thenRun(() -> this.pollingFutures.remove(pollingFuture));
		return pollingFuture;
	}

	protected Duration getPollTimeout() {
		return this.pollTimeout;
	}

	protected String getPollingEndpointName() {
		return this.pollingEndpointName;
	}

	protected int getMessagesPerPoll() {
		return this.messagesPerPoll;
	}

	@Override
	public void stop() {
		if (!isRunning()) {
			logger.debug("Message source for queue {} not running.", this.pollingEndpointName);
		}
		synchronized (this.lifecycleMonitor) {
			logger.debug("Stopping MessageSource for queue {}", this.pollingEndpointName);
			this.running = false;
			waitExistingTasksToFinish();
			doStop();
			this.pollingFutures.forEach(pollingFuture -> pollingFuture.cancel(true));
			logger.debug("MessageSource for queue {} stopped", this.pollingEndpointName);
		}
	}

	protected void doStop() {
	}

	private void waitExistingTasksToFinish() {
		Duration shutDownTimeout = this.shutdownTimeout;
		if (shutDownTimeout.isZero()) {
			logger.debug("Container shutdown timeout set to zero - not waiting for tasks to finish.");
			return;
		}
		boolean tasksFinished = this.backPressureHandler.drain(shutDownTimeout);
		if (!tasksFinished) {
			logger.warn("Tasks did not finish in {} seconds, proceeding with shutdown.", shutDownTimeout.getSeconds());
		}
	}

}
