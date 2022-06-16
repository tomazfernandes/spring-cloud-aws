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
import io.awspring.cloud.sqs.listener.acknowledgement.OnSuccessAckHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.Lifecycle;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessageListenerContainer<T> extends AbstractMessageListenerContainer<T> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainer.class);

	private final SqsAsyncClient asyncClient;

	private SqsContainerOptions containerOptions;

	private TaskExecutor taskExecutor;

	private Collection<AsyncMessagePoller<T>> messagePollers;

	private AsyncMessageListener<T> messageListener;

	private Semaphore pollersSemaphore;

	private AsyncErrorHandler<T> errorHandler = new LoggingErrorHandler<>();

	private AsyncAckHandler<T> ackHandler = new OnSuccessAckHandler<>();

	private MessageSplitter<T> splitter;

	private final Collection<AsyncMessageInterceptor<T>> messageInterceptors = new ArrayList<>();

	public SqsMessageListenerContainer(SqsAsyncClient asyncClient, SqsContainerOptions options) {
		super(options);
		this.asyncClient = asyncClient;
		this.containerOptions = options;
	}

	public void setErrorHandler(AsyncErrorHandler<T> errorHandler) {
		Assert.notNull(errorHandler, "errorHandler cannot be null");
		this.errorHandler = errorHandler;
	}

	public void setAckHandler(AsyncAckHandler<T> ackHandler) {
		Assert.notNull(ackHandler, "ackHandler cannot be null");
		this.ackHandler = ackHandler;
	}

	public void addMessageInterceptor(AsyncMessageInterceptor<T> messageInterceptor) {
		Assert.notNull(messageInterceptor, "messageInterceptor cannot be null");
		this.messageInterceptors.add(messageInterceptor);
	}

	public void addMessageInterceptors(Collection<AsyncMessageInterceptor<T>> messageInterceptors) {
		Assert.notNull(messageInterceptors, "messageInterceptors cannot be null");
		this.messageInterceptors.addAll(messageInterceptors);
	}

	@Override
	public void setMessageListener(AsyncMessageListener<T> asyncMessageListener) {
		this.messageListener = asyncMessageListener;
	}

	@Override
	protected void doStart() {
		this.messagePollers = doCreateMessagePollers(super.getQueueNames());
		Assert.notEmpty(this.messagePollers, () -> "MessagePollers cannot be empty: "
			+ this.containerOptions);
		Assert.notNull(this.messageListener, () -> "MessageListener cannot be empty:  "
			+ this.containerOptions);
		logger.debug("Starting container {}", super.getId());
		this.taskExecutor = createTaskExecutor();
		this.splitter = new OrderedSplitter<>(this.taskExecutor);
		this.pollersSemaphore = new Semaphore(this.containerOptions.getSimultaneousPolls());
		managePollersLifecycle(Lifecycle::start);
		this.taskExecutor.execute(this::pollAndProcessMessages);
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
					acquireSemaphore();
					if (!super.isRunning()) {
						logger.debug("Container not running, returning.");
						this.pollersSemaphore.release();
						return;
					}

					poller.poll(containerOptions.getMessagesPerPoll(), containerOptions.getPollTimeout())
						.thenCompose(msgs -> this.splitter.splitAndProcess(msgs, this::processMessage))
						.handle(handleProcessingResult())
						.thenRun(releaseSemaphore());
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					logger.trace("Thread interrupted", e);
				}
				catch (Exception e) {
					logger.error("Error in ListenerContainer {}", getId(), e);
				}
			});
		}
	}

	protected CompletableFuture<Void> processMessage(Message<T> message) {
		logger.trace("Processing message {}", message.getPayload());// MessageHeaderUtils.getId(message));
		return intercept(message)
			.thenCompose(this.messageListener::onMessage)
			.handle((val, t) -> handleResult(message, t))
			.thenCompose(x -> x);
	}

	private CompletableFuture<Message<T>> intercept(Message<T> message) {
		return this.messageInterceptors.stream().reduce(CompletableFuture.completedFuture(message),
			(messageFuture, interceptor) -> messageFuture.thenCompose(interceptor::intercept), (a, b) -> a);
	}

	protected CompletableFuture<Void> handleResult(Message<T> message, Throwable throwable) {
		logger.trace("Handling result for message {}", MessageHeaderUtils.getId(message));
		return throwable == null ? this.ackHandler.onSuccess(message)
			: this.errorHandler.handleError(message, throwable)
			.thenCompose(val -> this.ackHandler.onError(message, throwable));
	}

	private void acquireSemaphore() throws InterruptedException {
		this.pollersSemaphore.acquire();
		logger.trace("Semaphore acquired in container {} ", getId());
	}

	private Runnable releaseSemaphore() {
		return () -> {
			this.pollersSemaphore.release();
			logger.trace("Semaphore released in container {} ", getId());
		};
	}

	protected BiFunction<Object, Throwable, Void> handleProcessingResult() {
		return (value, t) -> {
			if (t != null) {
				logger.error("Error handling messages in container {} ", getId(), t);
			}
			return null;
		};
	}

	protected ThreadPoolTaskExecutor createTaskExecutor() {
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		int poolSize = this.containerOptions.getSimultaneousPolls() + 1;
		taskExecutor.setMaxPoolSize(poolSize);
		taskExecutor.setCorePoolSize(poolSize);
		taskExecutor.afterPropertiesSet();
		return taskExecutor;
	}

	protected Collection<AsyncMessagePoller<T>> doCreateMessagePollers(Collection<String> endpointNames) {
		return endpointNames.stream()
			.map(name -> new SqsMessagePoller<T>(name, this.asyncClient))
			.collect(Collectors.toList());
	}

	@Override
	protected void doStop() {
		managePollersLifecycle(Lifecycle::stop);
		if (this.taskExecutor instanceof DisposableBean) {
			try {
				((DisposableBean) this.taskExecutor).destroy();
			}
			catch (Exception e) {
				throw new IllegalStateException("Error shutting down TaskExecutor", e);
			}
		}
	}

	public void setQueueNames(String... queueNames) {
		super.setQueueNames(Arrays.asList(queueNames));
	}
}
