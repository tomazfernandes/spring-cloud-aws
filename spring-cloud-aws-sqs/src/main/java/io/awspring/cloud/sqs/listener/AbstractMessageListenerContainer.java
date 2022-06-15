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

import io.awspring.cloud.messaging.support.MessageHeaderUtils;
import io.awspring.cloud.messaging.support.listener.acknowledgement.AsyncAckHandler;
import io.awspring.cloud.messaging.support.listener.acknowledgement.OnSuccessAckHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.Lifecycle;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractMessageListenerContainer<T> implements MessageListenerContainer<T> {

	private static final Logger logger = LoggerFactory.getLogger(AbstractMessageListenerContainer.class);

	private final Object lifecycleMonitor = new Object();

	private final CommonContainerOptions<?> containerOptions;

	private volatile boolean isRunning;

	private String id;

	private TaskExecutor taskExecutor;

	private Semaphore pollersSemaphore;

	private Collection<AsyncMessagePoller<T>> messagePollers;

	private AsyncMessageListener<T> messageListener;

	private AsyncErrorHandler<T> errorHandler = new LoggingErrorHandler<>();

	private AsyncAckHandler<T> ackHandler = new OnSuccessAckHandler<>();

	private AsyncMessageInterceptor<T> messageInterceptor;

	private Collection<String> assignments;

	protected AbstractMessageListenerContainer(CommonContainerOptions<?> options) {
		Assert.notNull(options, "options cannot be null");
		this.containerOptions = options.createCopy();
	}

	public void setId(String id) {
		Assert.state(this.id == null, () -> "id already set for container " + this.id);
		this.id = id;
	}

	@Override
	public String getId() {
		return this.id;
	}

	public void setAssignments(Collection<String> assignments) {
		this.assignments = assignments;
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
	public boolean isRunning() {
		return this.isRunning;
	}

	@Override
	public void setMessageListener(AsyncMessageListener<T> asyncMessageListener) {
		this.messageListener = asyncMessageListener;
	}

	@Override
	public void start() {
		if (this.isRunning) {
			return;
		}
		synchronized (this.lifecycleMonitor) {
			this.isRunning = true;
			Assert.notEmpty(this.assignments, "No endpoint names set");
			if (CollectionUtils.isEmpty(this.messagePollers)) {
				this.messagePollers = doCreateMessagePollers(this.assignments);
			}
			Assert.notEmpty(this.messagePollers, () -> "MessagePollers cannot be empty: "
				+ this.containerOptions);
			Assert.notNull(this.messageListener, () -> "MessageListener cannot be empty:  "
				+ this.containerOptions);
			if (this.id == null) {
				this.id = resolveContainerId();
			}
			logger.debug("Starting container {}", this.id);
			doStart();
			this.taskExecutor = createTaskExecutor();
			this.pollersSemaphore = new Semaphore(this.containerOptions.getSimultaneousPolls());
			managePollersLifecycle(Lifecycle::start);
			this.taskExecutor.execute(this::pollAndProcessMessages);
		}
		logger.debug("Container started {}", this.id);
	}

	protected abstract Collection<AsyncMessagePoller<T>> doCreateMessagePollers(Collection<String> endpointNames);

	private String resolveContainerId() {
		return "io.awspring.cloud.sqs.sqsListenerEndpointContainer#" +
			this.messagePollers.stream()
				.filter(poller -> poller instanceof AbstractMessagePoller)
				.findFirst()
				.map(poller -> (((AbstractMessagePoller<?>) poller).getLogicalEndpointName()))
				.orElseGet(() -> UUID.randomUUID().toString());
	}

	protected void doStart() {
	}

	private void pollAndProcessMessages() {
		while (this.isRunning) {
			this.messagePollers.forEach(poller -> {
				try {
					acquireSemaphore();
					if (!this.isRunning) {
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
					logger.error("Error in ListenerContainer {}", this.id, e);
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
		logger.trace("Semaphore acquired in container {} ", this.id);
	}

	private Runnable releaseSemaphore() {
		return () -> {
			this.pollersSemaphore.release();
			logger.trace("Semaphore released in container {} ", this.id);
		};
	}

	protected BiFunction<Object, Throwable, Void> handleProcessingResult() {
		return (value, t) -> {
			if (t != null) {
				logger.error("Error handling messages in container {} ", this.id, t);
			}
			return null;
		};
	}

	private void managePollersLifecycle(Consumer<SmartLifecycle> consumer) {
		this.messagePollers.forEach(poller -> {
			if (poller instanceof SmartLifecycle) {
				consumer.accept((SmartLifecycle) poller);
			}
		});
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
	public void stop() {
		if (!this.isRunning) {
			return;
		}
		logger.debug("Stopping container {}", this.id);
		synchronized (this.lifecycleMonitor) {
			this.isRunning = false;
			doStop();
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
		logger.debug("Container stopped {}", this.id);
	}

	protected void doStop() {
	}
}
