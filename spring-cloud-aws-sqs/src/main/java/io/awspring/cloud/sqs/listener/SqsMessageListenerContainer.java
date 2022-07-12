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

import io.awspring.cloud.sqs.ConfigUtils;
import io.awspring.cloud.sqs.LifecycleUtils;
import io.awspring.cloud.sqs.listener.pipeline.AckHandlerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.ErrorHandlerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.InterceptorExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.MessageListenerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingContext;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingPipeline;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingPipelineBuilder;
import io.awspring.cloud.sqs.listener.pipeline.BackPressureReleaseStage;
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
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

	private int messagesPerPoll;

	private TaskExecutor taskExecutor;

	private Collection<MessageSource<T>> messageSources;

	private MessageSink<T> messageSink;

	private Set<CompletableFuture<?>> messageSourceFutures;

	private BackPressureHandler backPressureHandler;

	public SqsMessageListenerContainer(SqsAsyncClient asyncClient, ContainerOptions options) {
		super(options);
		this.asyncClient = asyncClient;
	}

	@Override
	protected void doStart() {
		this.messagesPerPoll = super.getContainerOptions().getMessagesPerPoll();
		this.messageSources = createMessageSources();
		this.messageSink = super.getMessageSink();
		this.taskExecutor = createContainerTaskExecutor();
		this.backPressureHandler = createBackPressureHandler();
		this.messageSourceFutures = Collections.synchronizedSet(new HashSet<>());
		configureComponents();
		LifecycleUtils.start(this.messageSink, this.messageSources);
		startContainerThread();
	}

	private SemaphoreBackPressureHandler createBackPressureHandler() {
		int totalPermits = getContainerOptions().getMaxInFlightMessagesPerQueue() * this.messageSources.size();
		return new SemaphoreBackPressureHandler(totalPermits, super.getContainerOptions().getSemaphoreAcquireTimeout());
	}

	private Collection<MessageSource<T>> createMessageSources() {
		return super.getMessageSourceFactory() != null
			? super.getMessageSourceFactory().create(getQueueNames())
			: SqsMessageSourceFactory.createSourcesFor(getQueueNames());
	}

	@SuppressWarnings("unchecked")
	private void configureComponents() {
		ConfigUtils.INSTANCE
			.acceptManyIfInstance(this.messageSources, PollableMessageSource.class, pms -> {
				pms.setPollTimeout(getContainerOptions().getPollTimeout());
				pms.setNumberOfMessagesPerPoll(getContainerOptions().getMessagesPerPoll());
			})
			.acceptManyIfInstance(this.messageSources, SqsMessageSource.class, sms -> sms.setSqsAsyncClient(this.asyncClient))
			.acceptIfInstance(this.messageSink, TaskExecutorAware.class, tea -> tea.setTaskExecutor(getOrCreateSinkTaskExecutor()))
			.acceptIfInstance(this.messageSink, MessageListeningSink.class, mls -> mls.setMessageListener(decorateMessageListener()));
	}

	private TaskExecutor getOrCreateSinkTaskExecutor() {
		return getContainerOptions().getSinkTaskExecutor() != null
			? getContainerOptions().getSinkTaskExecutor()
			: createSinkTaskExecutor();
	}

	private ThreadPoolTaskExecutor createSinkTaskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		int poolSize = getContainerOptions().getMaxInFlightMessagesPerQueue() * this.messageSources.size();
		executor.setMaxPoolSize(poolSize);
		executor.setCorePoolSize(poolSize);
		executor.setThreadNamePrefix(this.getClass().getSimpleName().toLowerCase() + "-");
		executor.afterPropertiesSet();
		return executor;
	}

	private AsyncMessageListener<T> decorateMessageListener() {
		return ProcessingPipelineMessageListenerAdapter.create(context -> context
			.interceptors(getMessageInterceptors()).messageListener(getMessageListener()).errorHandler(getErrorHandler())
			.ackHandler(getAckHandler()).backPressureHandler(this.backPressureHandler));
	}

	private void startContainerThread() {
		this.taskExecutor.execute(this::pollSourcesAndProcessMessages);
	}

	private void pollSourcesAndProcessMessages() {
		while (super.isRunning()) {
			this.messageSources.forEach(this::pollAndEmit);
		}
		logger.debug("Execution thread stopped.");
	}

	private void pollAndEmit(MessageSource<T> messageSource) {
		try {
			if (!isRunning() || !this.backPressureHandler.request(this.messagesPerPoll)) {
				return;
			}
			if (!isRunning()) {
				logger.debug("Container was stopped after acquiring permits. Releasing permits and returning.");
				this.backPressureHandler.release(this.messagesPerPoll);
				return;
			}
			manageSourceFuture(messageSource.receive()).exceptionally(this::handleSourceException)
				.thenApply(this::releaseUnusedPermits).thenCompose(this::emitMessagesToListener)
				.exceptionally(this::handleSinkException);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Container thread interrupted", e);
		} catch (Exception e) {
			logger.error("Error in ListenerContainer {}. Resuming.", getId(), e);
		}
	}

	public Collection<Message<T>> releaseUnusedPermits(Collection<Message<T>> msgs) {
		this.backPressureHandler.release(this.messagesPerPoll - msgs.size());
		return msgs;
	}

	private CompletableFuture<Void> emitMessagesToListener(Collection<Message<T>> messages) {
		return this.messageSink.emit(messages);
	}

	private Void handleSinkException(Throwable throwable) {
		logger.error("Sink returned an error.", throwable);
		return null;
	}

	private <F> CompletableFuture<F> manageSourceFuture(CompletableFuture<F> processingFuture) {
		this.messageSourceFutures.add(processingFuture);
		processingFuture.thenRun(() -> this.messageSourceFutures.remove(processingFuture));
		return processingFuture;
	}

	private Collection<Message<T>> handleSourceException(Throwable t) {
		logger.error("Error polling for messages in container {}", getId(), t);
		return Collections.emptyList();
	}

	private AsyncListenableTaskExecutor createContainerTaskExecutor() {
		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
		executor.setThreadNamePrefix(this.getId() + "-");
		return executor;
	}

	@Override
	protected void doStop() {
		LifecycleUtils.stop(this.messageSources);
		waitExistingTasksToFinish();
		this.messageSourceFutures.forEach(pollingFuture -> pollingFuture.cancel(true));
		LifecycleUtils.stop(this.messageSink);
		logger.debug("Container {} stopped", getId());
	}

	private void waitExistingTasksToFinish() {
		Duration shutDownTimeout = getContainerOptions().getShutDownTimeout();
		if (shutDownTimeout.isZero()) {
			logger.debug("Container shutdown timeout set to zero - not waiting for tasks to finish.");
			return;
		}
		boolean tasksFinished = this.backPressureHandler.drain(shutDownTimeout);
		if (!tasksFinished) {
			logger.warn("Tasks did not finish in {} seconds, proceeding with shutdown.", shutDownTimeout.getSeconds());
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
				.then(BackPressureReleaseStage::new)
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
