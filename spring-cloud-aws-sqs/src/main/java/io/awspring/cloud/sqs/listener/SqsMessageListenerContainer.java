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

import io.awspring.cloud.sqs.listener.acknowledgement.AckHandler;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.pipeline.AckHandlerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.ErrorHandlerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.InterceptorExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.MessageListenerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingContext;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingPipeline;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingPipelineBuilder;
import io.awspring.cloud.sqs.listener.pipeline.SemaphoreReleaseStage;
import io.awspring.cloud.sqs.listener.sink.AbstractMessageListeningSink;
import io.awspring.cloud.sqs.listener.sink.MessageListeningSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
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

	private MessageSink<T> messageSink;

	private CompletableFuture<?> executionFuture;

	private Duration semaphoreAcquireTimeout;

	private Set<CompletableFuture<Void>> messageProcessingFutures;

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
		this.messageProcessingFutures = Collections.synchronizedSet(new HashSet<>());
		this.inFlightMessagesSemaphore = new Semaphore(this.maxInFlightMessagesPerQueue * this.messageSources.size());
		this.processingPipelineMessageListener = decorateMessageListener();
		configureComponents();
		startComponents();
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
				.setTaskExecutor(getContainerOptions().getSinkTaskExecutor() != null
					? getContainerOptions().getSinkTaskExecutor() : createSinkTaskExecutor());
		}
		if (this.messageSink instanceof SmartInitializingSingleton) {
			((SmartInitializingSingleton) this.messageSink).afterSingletonsInstantiated();
		}
		if (this.messageSink instanceof MessageListeningSink) {
			((MessageListeningSink<T>) this.messageSink).setMessageListener(this.processingPipelineMessageListener);
		}
	}

	private void startComponents() {
		if (this.messageSink instanceof SmartLifecycle) {
			((SmartLifecycle) this.messageSink).start();
		}
		manageSourcesLifecycle(Lifecycle::start);
	}

	private TaskExecutor createSinkTaskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		int poolSize = this.maxInFlightMessagesPerQueue * this.messageSources.size();
		executor.setMaxPoolSize(poolSize);
		executor.setCorePoolSize(poolSize);
		executor.setThreadNamePrefix(this.getClass().getSimpleName().toLowerCase() + "-");
		executor.afterPropertiesSet();
		return executor;
	}

	private AsyncMessageListener<T> decorateMessageListener() {
		return new ProcessingPipelineMessageListenerAdapter(super.getMessageListener(), super.getErrorHandler(),
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
		} else {
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
						.thenApply(this::releaseUnusedPermits).thenCompose(this::emitMessagesToListener)
						.exceptionally(this::handleSinkException));
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

	private void releasePermits(int numberOfPermits) {
		logger.trace("Releasing {} permits", numberOfPermits);
		this.inFlightMessagesSemaphore.release(numberOfPermits);
	}

	private void manageFutureFrom(CompletableFuture<Void> processingFuture) {
		this.messageProcessingFutures.add(processingFuture);
		processingFuture.thenRun(() -> this.messageProcessingFutures.remove(processingFuture));
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
		manageSourcesLifecycle(Lifecycle::stop);
		// TODO: Make waiting optional
		waitExistingTasksToFinish();
		this.messageProcessingFutures.forEach(pollingFuture -> pollingFuture.cancel(true));
		if (this.messageSink instanceof SmartLifecycle) {
			((SmartLifecycle) this.messageSink).stop();
		}
		if (this.executionFuture != null) {
			this.executionFuture.thenRun(this::shutdownTaskExecutor);
		} else {
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
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while waiting for container tasks to finish", e);
		}
	}

	private void shutdownTaskExecutor() {
		if (this.taskExecutor instanceof DisposableBean) {
			try {
				((DisposableBean) this.taskExecutor).destroy();
			} catch (Exception e) {
				throw new IllegalStateException("Error shutting down TaskExecutor", e);
			}
		}
	}

	private class ProcessingPipelineMessageListenerAdapter implements AsyncMessageListener<T> {

		private final MessageProcessingPipeline<T> pipeline;

		private ProcessingPipelineMessageListenerAdapter(AsyncMessageListener<T> messageListener,
														 AsyncErrorHandler<T> errorHandler, AckHandler<T> ackHandler,
														 Collection<AsyncMessageInterceptor<T>> messageInterceptors) {
			Assert.notNull(messageListener, "messageListener cannot be null provided");
			Assert.notNull(errorHandler, "No error handler provided");
			Assert.notNull(ackHandler, "No ackHandler provided");
			Assert.notNull(messageInterceptors, "messageInterceptors cannot be null");
			this.pipeline = MessageProcessingPipelineBuilder
				.<T>first(InterceptorExecutionStage::new)
				.then(MessageListenerExecutionStage::new)
				.wrappedWith(ErrorHandlerExecutionStage::new)
				.wrappedWith(AckHandlerExecutionStage::new)
				.then(SemaphoreReleaseStage::new)
				.build(MessageProcessingContext.<T>builder().interceptors(messageInterceptors)
					.messageListener(messageListener).errorHandler(errorHandler)
					.ackHandler(ackHandler).semaphore(SqsMessageListenerContainer.this.inFlightMessagesSemaphore).build());
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
