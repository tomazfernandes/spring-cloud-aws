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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
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

	private AsyncMessageInterceptor<T> messageInterceptor;


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

	public void setMessageInterceptor(AsyncMessageInterceptor<T> messageInterceptor) {
		Assert.notNull(messageInterceptor, "messageInterceptor cannot be null");
		this.messageInterceptor = messageInterceptor;
	}

	public void setMessagePollers(Collection<AsyncMessagePoller<T>> messagePollers) {
		Assert.notEmpty(messagePollers, "messagePollers cannot be null");
		this.messagePollers = messagePollers;
	}

	public void setMessagePoller(AsyncMessagePoller<T> messagePoller) {
		Assert.notNull(messagePoller, "messagePoller cannot be null");
		this.messagePollers = Collections.singletonList(messagePoller);
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
						.thenCompose(this::splitAndProcessMessages)
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

	protected CompletableFuture<?> splitAndProcessMessages(Collection<Message<T>> messages) {
		logger.trace("Received {} messages", messages.size());
		return CompletableFuture
			.allOf(messages.stream().map(this::processMessageAsync).toArray(CompletableFuture[]::new));
	}

	protected CompletableFuture<?> processMessageAsync(Message<T> message) {
		logger.trace("Received message {}", MessageHeaderUtils.getId(message));
		return CompletableFuture.supplyAsync(() -> doProcessMessage(message), this.taskExecutor)
			.thenCompose(x -> x);
	}

	protected CompletableFuture<?> doProcessMessage(Message<T> message) {
		logger.trace("Processing message {}", MessageHeaderUtils.getId(message));
		return maybeIntercept(message, this.messageListener::onMessage)
			.handle((val, t) -> handleResult(message, t))
			.thenCompose(x -> x);
	}

	protected CompletableFuture<?> handleResult(Message<T> message, Throwable throwable) {
		logger.trace("Handling result for message {}", MessageHeaderUtils.getId(message));
		return throwable == null ? this.ackHandler.onSuccess(message)
			: this.errorHandler.handleError(message, throwable)
			.thenCompose(val -> this.ackHandler.onError(message, throwable));
	}

	private CompletableFuture<Void> maybeIntercept(Message<T> message,
												   Function<Message<T>, CompletableFuture<Void>> listener) {
		logger.trace("Evaluating interceptor for message {}", MessageHeaderUtils.getId(message));
		return this.messageInterceptor != null
			? this.messageInterceptor.intercept(message).thenCompose(listener)
			: listener.apply(message);
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


	@Override
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
