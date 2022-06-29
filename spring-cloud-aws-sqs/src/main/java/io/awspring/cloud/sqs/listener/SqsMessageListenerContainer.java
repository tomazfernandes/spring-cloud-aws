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
import io.awspring.cloud.sqs.listener.acknowledgement.AckHandler;
import io.awspring.cloud.sqs.listener.adapter.DelegatingMessageListenerAdapter;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.sink.AbstractMessageListeningSink;
import io.awspring.cloud.sqs.listener.sink.MessageListeningSink;
import io.awspring.cloud.sqs.listener.source.MessageSource;
import io.awspring.cloud.sqs.listener.source.MessageSourceFactory;
import io.awspring.cloud.sqs.listener.source.PollableMessageSource;
import io.awspring.cloud.sqs.listener.source.SqsMessageSource;
import io.awspring.cloud.sqs.listener.source.SqsMessageSourceFactory;
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
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.Lifecycle;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * {@link MessageListenerContainer} implementation for SQS queues.
 *
 * Components and {@link ContainerOptions} can be changed at runtime and such changes will be valid upon container
 * restart.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessageListenerContainer<T> extends AbstractMessageListenerContainer<T> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainer.class);

	private final SqsAsyncClient asyncClient;

	private int maxInFlightMessagesPerQueue;

	private int messagesPerPoll;

	private TaskExecutor taskExecutor;

	private Semaphore inFlightMessagesSemaphore;

	private Collection<MessageSource<T>> messageSources;

	private MessageListeningSink<T> messageSink;

	private CompletableFuture<?> executionFuture;

	private Duration semaphoreAcquireTimeout;

	private Set<CompletableFuture<Void>> pollingFutures;

	private AsyncMessageListener<T> decoratedMessageListener;

	public SqsMessageListenerContainer(SqsAsyncClient asyncClient, ContainerOptions options) {
		super(options);
		this.asyncClient = asyncClient;
	}

	@Override
	protected void doStart() {
		this.semaphoreAcquireTimeout = super.getContainerOptions().getSemaphoreAcquireTimeout();
		this.messagesPerPoll = super.getContainerOptions().getMessagesPerPoll();
		this.maxInFlightMessagesPerQueue = super.getContainerOptions().getMaxInFlightMessagesPerQueue();
		this.messageSources = createMessageSources();
		this.messageSink = super.getMessageSink();
		this.taskExecutor = createTaskExecutor();
		this.pollingFutures = Collections.synchronizedSet(new HashSet<>());
		this.decoratedMessageListener = decorateMessageListener();
		this.inFlightMessagesSemaphore = new Semaphore(this.maxInFlightMessagesPerQueue * this.messageSources.size());
		logger.debug("Starting container {}", super.getId());
		configureComponents();
		manageSourcesLifecycle(Lifecycle::start);
		if (this.messageSink instanceof SmartLifecycle) {
			((SmartLifecycle) this.messageSink).start();
		}
		startContainerThread();
	}

	private Collection<MessageSource<T>> createMessageSources() {
		MessageSourceFactory<T> factoryToUse = super.getMessageSourceFactory() != null ? super.getMessageSourceFactory()
				: new SqsMessageSourceFactory<>();
		return this.getQueueNames().stream().map(factoryToUse::create).collect(Collectors.toList());
	}

	private void configureComponents() {
		configureMessageSources();
		configureSink();
	}

	private void configureMessageSources() {
		this.messageSources.forEach(source -> {
			if (source instanceof PollableMessageSource) {
				PollableMessageSource<?> pollableSource = (PollableMessageSource<?>) source;
				pollableSource.setPollTimeout(super.getContainerOptions().getPollTimeout());
				pollableSource.setNumberOfMessagesPerPoll(super.getContainerOptions().getMessagesPerPoll());
			}
			if (source instanceof SqsMessageSource) {
				((SqsMessageSource<?>) source).setSqsAsyncClient(this.asyncClient);
			}
		});
	}

	private void configureSink() {
		if (this.messageSink instanceof AbstractMessageListeningSink) {
			((AbstractMessageListeningSink<T>) this.messageSink)
					.setPoolSize(this.maxInFlightMessagesPerQueue * this.messageSources.size());
		}
		if (this.messageSink instanceof SmartInitializingSingleton) {
			((SmartInitializingSingleton) this.messageSink).afterSingletonsInstantiated();
		}
	}

	private ProcessingPipelineMessageListener<T> decorateMessageListener() {
		return new ProcessingPipelineMessageListener<>(super.getMessageListener(), super.getErrorHandler(),
				super.getAckHandler(), super.getMessageInterceptors());
	}

	private void manageSourcesLifecycle(Consumer<SmartLifecycle> consumer) {
		this.messageSources.forEach(poller -> {
			if (poller instanceof SmartLifecycle) {
				consumer.accept((SmartLifecycle) poller);
			}
		});
	}

	private void startContainerThread() {
		if (this.taskExecutor instanceof AsyncListenableTaskExecutor) {
			this.executionFuture = ((AsyncListenableTaskExecutor) this.taskExecutor)
					.submitListenable(this::pollAndProcessMessages).completable();
		}
		else {
			this.taskExecutor.execute(this::pollAndProcessMessages);
		}
	}

	private void pollAndProcessMessages() {
		while (super.isRunning()) {
			this.messageSources.forEach(messageSource -> {
				try {
					if (!acquirePermits()) {
						return;
					}
					if (!super.isRunning()) {
						logger.debug("Container not running. Returning.");
						releasePermits(this.messagesPerPoll);
						return;
					}
					manageFutureFrom(messageSource.receive().exceptionally(this::handleSourceException)
							.thenApply(this::releaseUnusedPermits).thenApply(this::emitMessagesToListener)
							.thenAccept(this::releasePermitsAndHandleResult));
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

	private Collection<CompletableFuture<Void>> emitMessagesToListener(Collection<Message<T>> messages) {
		return this.messageSink.emit(messages, this.decoratedMessageListener);
	}

	private void releasePermitsAndHandleResult(Collection<CompletableFuture<Void>> messageExecutionFutures) {
		messageExecutionFutures.forEach(future -> future.exceptionally(t -> {
			logger.error("Error processing message: ", t);
			return null;
		}).thenRun(() -> releasePermits(1)));
	}

	private void releasePermits(int messagesPerPoll) {
		logger.trace("Releasing {} permits", messagesPerPoll);
		this.inFlightMessagesSemaphore.release(messagesPerPoll);
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

	protected Collection<Message<T>> handleSourceException(Throwable t) {
		logger.error("Error polling for messages", t);
		return Collections.emptyList();
	}

	private TaskExecutor createTaskExecutor() {
		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
		executor.setThreadNamePrefix(this.getId() + "-");
		return executor;
	}

	@Override
	protected void doStop() {
		manageSourcesLifecycle(Lifecycle::stop);
		// TODO: Make waiting optional
		waitExistingTasksToFinish();
		this.pollingFutures.forEach(pollingFuture -> pollingFuture.cancel(true));
		if (this.messageSink instanceof SmartLifecycle) {
			((SmartLifecycle) this.messageSink).stop();
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
			int totalPermits = this.maxInFlightMessagesPerQueue * this.messageSources.size();
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

	private static class ProcessingPipelineMessageListener<T> extends DelegatingMessageListenerAdapter<T> {

		private final AsyncErrorHandler<T> errorHandler;
		private final AckHandler<T> ackHandler;
		private final Collection<AsyncMessageInterceptor<T>> messageInterceptors;

		private ProcessingPipelineMessageListener(AsyncMessageListener<T> messageListener,
				AsyncErrorHandler<T> errorHandler, AckHandler<T> ackHandler,
				Collection<AsyncMessageInterceptor<T>> messageInterceptors) {
			super(messageListener);
			Assert.notNull(errorHandler, "No error handler provided");
			Assert.notNull(ackHandler, "No ackHandler provided");
			Assert.notNull(messageInterceptors, "MessageInterceptors cannot be null");
			this.errorHandler = errorHandler;
			this.ackHandler = ackHandler;
			this.messageInterceptors = messageInterceptors;
		}

		@Override
		public CompletableFuture<Void> onMessage(Message<T> message) {
			logger.trace("Processing message {}", MessageHeaderUtils.getId(message));
			return intercept(message).thenCompose(getDelegate()::onMessage)
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
	}

}
