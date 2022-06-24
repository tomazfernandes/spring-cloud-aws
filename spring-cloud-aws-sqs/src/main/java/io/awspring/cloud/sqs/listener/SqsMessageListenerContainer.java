/*
 * Copyright 2013-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.awspring.cloud.sqs.listener;

import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.acknowledgement.AsyncAckHandler;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.poller.AsyncMessagePoller;
import io.awspring.cloud.sqs.listener.poller.SqsMessagePoller;
import io.awspring.cloud.sqs.listener.splitter.AbstractMessageSplitter;
import io.awspring.cloud.sqs.listener.splitter.AsyncMessageSplitter;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.Lifecycle;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * {@link MessageListenerContainer} implementation for SQS queues.
 *
 * All properties are assigned on container {@link #start()}, thus components and {@link ContainerOptions} can be
 * changed at runtime and such changes will be valid upon container restart.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessageListenerContainer<T> extends AbstractMessageListenerContainer<T> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainer.class);

	private final SqsAsyncClient asyncClient;

	private int maxInFlightMessagesPerQueue;

	private int messagesPerPoll;

	private Duration pollTimeout;

	private TaskExecutor taskExecutor;

	private Semaphore inFlightMessagesSemaphore;

	private Collection<AsyncMessagePoller<T>> messagePollers;

	private AsyncMessageListener<T> messageListener;

	private AsyncErrorHandler<T> errorHandler;

	private AsyncAckHandler<T> ackHandler;

	private AsyncMessageSplitter<T> splitter;

	private Collection<AsyncMessageInterceptor<T>> messageInterceptors;

	private CompletableFuture<?> executionFuture;

	private Duration semaphoreAcquireTimeout;

	private Set<CompletableFuture<Void>> pollingFutures;

	public SqsMessageListenerContainer(SqsAsyncClient asyncClient, ContainerOptions options) {
		super(options);
		this.asyncClient = asyncClient;
	}

	@Override
	protected void doStart() {
		this.semaphoreAcquireTimeout = super.getContainerOptions().getSemaphoreAcquireTimeout();
		this.messagesPerPoll = super.getContainerOptions().getMessagesPerPoll();
		this.pollTimeout = super.getContainerOptions().getPollTimeout();
		this.maxInFlightMessagesPerQueue = super.getContainerOptions().getMaxInFlightMessagesPerQueue();
		this.pollingFutures = Collections.synchronizedSet(new HashSet<>());
		this.messagePollers = !super.getMessagePollers().isEmpty() ? super.getMessagePollers() : createMessagePollers();
		this.splitter = super.getSplitter();
		this.messageListener = super.getMessageListener();
		this.errorHandler = super.getErrorHandler();
		this.ackHandler = super.getAckHandler();
		this.messageInterceptors = super.getMessageInterceptors();
		this.inFlightMessagesSemaphore = new Semaphore(this.maxInFlightMessagesPerQueue * this.messagePollers.size());
		this.taskExecutor = createTaskExecutor();
		Assert.notNull(this.messageListener, "MessageListener cannot be null");
		logger.debug("Starting container {}", super.getId());
		managePollersLifecycle(Lifecycle::start);
		startSplitter();
		if (this.taskExecutor instanceof AsyncListenableTaskExecutor) {
			this.executionFuture = ((AsyncListenableTaskExecutor) this.taskExecutor)
					.submitListenable(this::pollAndProcessMessages).completable();
		}
		else {
			this.taskExecutor.execute(this::pollAndProcessMessages);
		}
	}

	private void startSplitter() {
		if (this.splitter instanceof AbstractMessageSplitter) {
			((AbstractMessageSplitter<T>) this.splitter)
					.setCoreSize(this.maxInFlightMessagesPerQueue * this.messagePollers.size());
		}
		if (this.splitter instanceof SmartLifecycle) {
			((SmartLifecycle) this.splitter).start();
		}
	}

	private void managePollersLifecycle(Consumer<SmartLifecycle> consumer) {
		this.messagePollers.forEach(poller -> {
			if (poller instanceof SmartLifecycle) {
				consumer.accept((SmartLifecycle) poller);
			}
		});
	}

	private void pollAndProcessMessages() {
		while (super.isRunning()) {
			this.messagePollers.forEach(poller -> {
				try {
					if (!acquirePermits()) {
						return;
					}
					if (!super.isRunning()) {
						logger.debug("Container not running. Returning.");
						releasePermits();
						return;
					}
					manageFutureFrom(poller.poll(this.messagesPerPoll, this.pollTimeout)
							.exceptionally(this::handlePollingException).thenApply(this::releaseUnusedPermits)
							.thenAccept(msgs -> this.splitter.splitAndProcess(msgs, this::processMessage)
									.forEach(this::releasePermitAndHandleResult)));
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException("Container thread interrupted", e);
				}
				catch (Exception e) {
					logger.error("Error in ListenerContainer {}. Resuming.", getId(), e);
				}
			});
		}
		logger.debug("Execution thread stopped.");
	}

	private void releasePermits() {
		this.inFlightMessagesSemaphore.release(this.messagesPerPoll);
	}

	private void manageFutureFrom(CompletableFuture<Void> pollingFuture) {
		this.pollingFutures.add(pollingFuture);
		pollingFuture.thenRun(() -> this.pollingFutures.remove(pollingFuture));
	}

	private boolean acquirePermits() throws InterruptedException {
		if (!isRunning()) {
			return false;
		}
		logger.trace("Acquiring {} permits in container {}", this.messagesPerPoll, getId());
		boolean hasAcquired = this.inFlightMessagesSemaphore.tryAcquire(this.messagesPerPoll,
				this.semaphoreAcquireTimeout.getSeconds(), TimeUnit.SECONDS);
		if (hasAcquired) {
			logger.trace("{} permits acquired in container {} ", this.messagesPerPoll, getId());
		}
		else {
			logger.trace("Not able to acquire permits in {} seconds. Skipping.",
					this.semaphoreAcquireTimeout.getSeconds());
		}
		return hasAcquired;
	}

	private Collection<Message<T>> releaseUnusedPermits(Collection<Message<T>> msgs) {
		this.inFlightMessagesSemaphore.release(this.messagesPerPoll - msgs.size());
		return msgs;
	}

	private void releasePermitAndHandleResult(CompletableFuture<Void> processingPipelineFuture) {
		processingPipelineFuture.handle(this::handleProcessingResult);
	}

	protected Collection<Message<T>> handlePollingException(Throwable t) {
		logger.error("Error polling for messages", t);
		return Collections.emptyList();
	}

	private CompletableFuture<Void> processMessage(Message<T> message) {
		logger.trace("Processing message {}", MessageHeaderUtils.getId(message));
		return intercept(message).thenCompose(this.messageListener::onMessage)
				.handle((val, t) -> handleMessageListenerResult(message, t)).thenCompose(x -> x);
	}

	private CompletableFuture<Message<T>> intercept(Message<T> message) {
		return this.messageInterceptors.stream().reduce(CompletableFuture.completedFuture(message),
				(messageFuture, interceptor) -> messageFuture.thenCompose(interceptor::intercept), (a, b) -> a);
	}

	private CompletableFuture<Void> handleMessageListenerResult(Message<T> message, Throwable throwable) {
		logger.trace("Handling result for message {}", MessageHeaderUtils.getId(message));
		return throwable == null ? this.ackHandler.onSuccess(message)
				: this.errorHandler.handleError(message, throwable)
						.thenCompose(val -> this.ackHandler.onError(message, throwable));
	}

	private Object handleProcessingResult(Object value, @Nullable Throwable t) {
		this.inFlightMessagesSemaphore.release();
		if (t != null) {
			logger.error("Error handling messages in container {} ", getId(), t);
		}
		return null;
	}

	protected TaskExecutor createTaskExecutor() {
		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
		executor.setThreadNamePrefix(this.getId() + "-");
		return executor;
	}

	private Collection<AsyncMessagePoller<T>> createMessagePollers() {
		return super.getQueueNames().stream().map(name -> new SqsMessagePoller<T>(name, this.asyncClient))
				.collect(Collectors.toList());
	}

	@Override
	protected void doStop() {
		managePollersLifecycle(Lifecycle::stop);
		// TODO: Make waiting optional
		waitExistingTasksToFinish();
		this.pollingFutures.forEach(pollingFuture -> pollingFuture.cancel(true));
		if (this.splitter instanceof SmartLifecycle) {
			((SmartLifecycle) this.splitter).stop();
		}
		if (this.executionFuture != null) {
			this.executionFuture.thenRun(this::shutdownTaskExecutor);
		}
		else {
			shutdownTaskExecutor();
		}
		logger.debug("Container {} stopped", getId());
	}

	private void waitExistingTasksToFinish() {
		try {
			int timeoutSeconds = 20; // TODO: Make timeout configurable
			int totalPermits = this.maxInFlightMessagesPerQueue * this.messagePollers.size();
			logger.debug("Waiting for up to {} seconds for approx. {} tasks to finish on container {}", timeoutSeconds,
					totalPermits - this.inFlightMessagesSemaphore.availablePermits(), this.getId());
			boolean tasksFinished = this.inFlightMessagesSemaphore.tryAcquire(totalPermits, timeoutSeconds,
					TimeUnit.SECONDS);
			if (!tasksFinished) {
				logger.warn("Tasks did not finish in {} seconds, proceeding with shutdown for container {}",
						timeoutSeconds, getId());
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while waiting for container tasks to finish", e);
		}
	}

	private void shutdownTaskExecutor() {
		if (this.taskExecutor instanceof DisposableBean) {
			try {
				((DisposableBean) this.taskExecutor).destroy();
			}
			catch (Exception e) {
				throw new IllegalStateException("Error shutting down TaskExecutor", e);
			}
		}
	}

}
