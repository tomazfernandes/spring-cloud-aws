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

import io.awspring.cloud.sqs.JavaUtils;
import io.awspring.cloud.sqs.LifecycleUtils;
import io.awspring.cloud.sqs.listener.pipeline.AckHandlerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.ErrorHandlerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.InterceptorExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.MessageListenerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingContext;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingPipeline;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingPipelineBuilder;
import io.awspring.cloud.sqs.listener.pipeline.SemaphoreReleaseStage;
import io.awspring.cloud.sqs.listener.sink.MessageListeningSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.sink.TaskExecutorAware;
import io.awspring.cloud.sqs.listener.source.MessageSource;
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
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
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

	private MessageSink<T> messageSink;

	private CompletableFuture<?> executionFuture;

	private Duration semaphoreAcquireTimeout;

	private Set<CompletableFuture<?>> pollingFutures;

	private AsyncMessageListener<T> processingPipelineMessageListener;

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
		this.taskExecutor = createContainerTaskExecutor();
		this.pollingFutures = Collections.synchronizedSet(new HashSet<>());
		this.inFlightMessagesSemaphore = new Semaphore(this.maxInFlightMessagesPerQueue * this.messageSources.size());
		this.processingPipelineMessageListener = decorateMessageListener();
		configureComponents();
		LifecycleUtils.start(this.messageSink, this.messageSources);
		startContainerThread();
	}

	private Collection<MessageSource<T>> createMessageSources() {
		return super.getMessageSourceFactory() != null
			? super.getMessageSourceFactory().create(this.getQueueNames())
			: SqsMessageSourceFactory.createSourcesFor(this.getQueueNames());
	}

	@SuppressWarnings("unchecked")
	private void configureComponents() {
		JavaUtils.INSTANCE
			.executeManyIfInstance(this.messageSources, PollableMessageSource.class, pms -> {
				pms.setPollTimeout(getContainerOptions().getPollTimeout());
				pms.setNumberOfMessagesPerPoll(getContainerOptions().getMessagesPerPoll());
			})
			.executeManyIfInstance(this.messageSources, SqsMessageSource.class, sms -> sms.setSqsAsyncClient(this.asyncClient))
			.acceptIfInstance(this.messageSink, TaskExecutorAware.class, tea -> tea.setTaskExecutor(getOrCreateSinkTaskExecutor()))
			.acceptIfInstance(this.messageSink, MessageListeningSink.class, mls -> mls.setMessageListener(this.processingPipelineMessageListener));
	}

	private TaskExecutor getOrCreateSinkTaskExecutor() {
		return getContainerOptions().getSinkTaskExecutor() != null
			? getContainerOptions().getSinkTaskExecutor()
			: createSinkTaskExecutor();
	}

	private ThreadPoolTaskExecutor createSinkTaskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		int poolSize = this.maxInFlightMessagesPerQueue * this.messageSources.size();
		executor.setMaxPoolSize(poolSize);
		executor.setCorePoolSize(poolSize);
		executor.setThreadNamePrefix(this.getClass().getSimpleName().toLowerCase() + "-");
		executor.afterPropertiesSet();
		return executor;
	}

	private AsyncMessageListener<T> decorateMessageListener() {
		return ProcessingPipelineMessageListenerAdapter.create(context -> context
			.interceptors(super.getMessageInterceptors()).messageListener(super.getMessageListener()).errorHandler(super.getErrorHandler())
			.ackHandler(super.getAckHandler()).semaphore(this.inFlightMessagesSemaphore));
	}

	private void startContainerThread() {
		JavaUtils.INSTANCE.executeIfInstanceOtherwise(this.taskExecutor, AsyncListenableTaskExecutor.class,
				listenableExecutor -> this.executionFuture = listenableExecutor.submitListenable(this::pollAndProcessMessages).completable(),
			taskExecutor -> taskExecutor.execute(this::pollAndProcessMessages));
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
					manageFutureFrom(messageSource.receive()).exceptionally(this::handleSourceException)
						.thenApply(this::releaseUnusedPermits).thenCompose(this::emitMessagesToListener)
						.exceptionally(this::handleSinkException);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException("Container thread interrupted", e);
				} catch (Exception e) {
					logger.error("Error in ListenerContainer {}. Resuming.", getId(), e);
				}
			});
		}
		logger.debug("Execution thread stopped.");
	}

	private CompletableFuture<Void> emitMessagesToListener(Collection<Message<T>> messages) {
		return this.messageSink.emit(messages);
	}

	private Void handleSinkException(Throwable throwable) {
		logger.error("Sink returned an error.", throwable);
		return null;
	}

	private <F> CompletableFuture<F> manageFutureFrom(CompletableFuture<F> processingFuture) {
		this.pollingFutures.add(processingFuture);
		processingFuture.thenRun(() -> this.pollingFutures.remove(processingFuture));
		return processingFuture;
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
			logger.trace("Permits left: {}", this.inFlightMessagesSemaphore.availablePermits());
		} else {
			logger.trace("Not able to acquire permits in {} seconds. Skipping.",
				this.semaphoreAcquireTimeout.getSeconds());
		}
		return hasAcquired;
	}

	private Collection<Message<T>> releaseUnusedPermits(Collection<Message<T>> msgs) {
		this.inFlightMessagesSemaphore.release(this.messagesPerPoll - msgs.size());
		return msgs;
	}

	private void releasePermits(int numberOfPermits) {
		logger.trace("Releasing {} permits", numberOfPermits);
		this.inFlightMessagesSemaphore.release(numberOfPermits);
	}

	private Collection<Message<T>> handleSourceException(Throwable t) {
		logger.error("Error polling for messages in container {}", getId(), t);
		return Collections.emptyList();
	}

	private TaskExecutor createContainerTaskExecutor() {
		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
		executor.setThreadNamePrefix(this.getId() + "-");
		return executor;
	}

	@Override
	protected void doStop() {
		LifecycleUtils.stop(this.messageSources);
		waitExistingTasksToFinish();
		this.pollingFutures.forEach(pollingFuture -> pollingFuture.cancel(true));
		LifecycleUtils.stop(this.messageSink);
		shutDownTaskExecutor();
		logger.debug("Container {} stopped", getId());
	}

	private void shutDownTaskExecutor() {
		if (this.executionFuture != null) {
			this.executionFuture.thenRun(this::doShutdownTaskExecutor);
		} else {
			doShutdownTaskExecutor();
		}
	}

	private void waitExistingTasksToFinish() {
		try {
			if (getContainerOptions().getShutDownTimeout().isZero()) {
				logger.debug("Container shutdown timeout set to zero - not waiting for tasks to finish.");
				return;
			}
			int timeoutSeconds = (int) getContainerOptions().getShutDownTimeout().getSeconds();
			int totalPermits = this.maxInFlightMessagesPerQueue * this.messageSources.size();
			logger.debug("Waiting for up to {} seconds for approx. {} tasks to finish on container {}", timeoutSeconds,
				totalPermits - this.inFlightMessagesSemaphore.availablePermits(), this.getId());
			boolean tasksFinished = this.inFlightMessagesSemaphore.tryAcquire(totalPermits, timeoutSeconds,
				TimeUnit.SECONDS);
			if (!tasksFinished) {
				logger.warn("Tasks did not finish in {} seconds, proceeding with shutdown for container {}",
					timeoutSeconds, getId());
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while waiting for container tasks to finish", e);
		}
	}

	private void doShutdownTaskExecutor() {
		if (this.taskExecutor instanceof DisposableBean) {
			try {
				((DisposableBean) this.taskExecutor).destroy();
			} catch (Exception e) {
				throw new IllegalStateException("Error shutting down TaskExecutor", e);
			}
		}
	}

	private static class ProcessingPipelineMessageListenerAdapter<T> implements AsyncMessageListener<T> {

		private final MessageProcessingPipeline<T> pipeline;

		private ProcessingPipelineMessageListenerAdapter(MessageProcessingContext<T> context) {
			this.pipeline = MessageProcessingPipelineBuilder
				.<T>first(InterceptorExecutionStage::new)
				.then(MessageListenerExecutionStage::new)
				.wrappedWith(ErrorHandlerExecutionStage::new)
				.wrappedWith(AckHandlerExecutionStage::new)
				.then(SemaphoreReleaseStage::new)
				.build(context);
		}

		static <T> ProcessingPipelineMessageListenerAdapter<T> create(Function<MessageProcessingContext.Builder<T>,
			MessageProcessingContext.Builder<T>> contextConsumer) {
			return new ProcessingPipelineMessageListenerAdapter<>(contextConsumer.apply(MessageProcessingContext.builder()).build());
		}

		@Override
		public CompletableFuture<Void> onMessage(Message<T> message) {
			return this.pipeline.process(message).thenRun(() -> {});
		}

		@Override
		public CompletableFuture<Void> onMessage(Collection<Message<T>> messages) {
			return this.pipeline.process(messages).thenRun(() -> {});
		}
	}

}
