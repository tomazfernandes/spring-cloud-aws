/*
 * Copyright 2022 the original author or authors.
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
package io.awspring.cloud.sqs.listener.sink;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * Base implementation for {@link MessageListeningSink} containing {@link SmartLifecycle} features
 * and useful execution methods that can be used by subclasses.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractMessageListeningSink<T> implements MessageListeningSink<T>, TaskExecutorAwareComponent {

	private static final Logger logger = LoggerFactory.getLogger(AbstractMessageListeningSink.class);

	private final Object lifecycleMonitor = new Object();

	private volatile boolean running;

	private TaskExecutor taskExecutor;

	private AsyncMessageListener<T> messageListener;

	@Override
	public void setMessageListener(AsyncMessageListener<T> messageListener) {
		Assert.notNull(messageListener, "listener must not be null.");
		this.messageListener = messageListener;
	}

	protected AsyncMessageListener<T> getMessageListener() {
		return this.messageListener;
	}

	@Override
	public CompletableFuture<MessageProcessingResult> emit(Collection<Message<T>> messages, MessageProcessingContext<T> context) {
		Assert.notNull(messages, "messages cannot be null");
		if (!isRunning()) {
			logger.debug("Sink not running, returning");
			return CompletableFuture.completedFuture(null);
		}
		if (messages.size() == 0) {
			logger.debug("No messages provided, returning.");
			return CompletableFuture.completedFuture(null);
		}
		return doEmit(messages, context);
	}

	protected abstract CompletableFuture<MessageProcessingResult> doEmit(Collection<Message<T>> messages, MessageProcessingContext<T> context);

	protected CompletableFuture<MessageProcessingResult> execute(Message<T> message, MessageProcessingContext<T> context) {
		Assert.state(this.taskExecutor != null, "TaskExecutor cannot be null");
		return CompletableFuture.supplyAsync(() -> getMessageListener().onMessage(message), this.taskExecutor)
			.thenCompose(Function.identity())
			.thenApply(theVoid -> MessageProcessingResult.successfulMessage(message))
			.exceptionally(t -> handleListenerError(Collections.singletonList(message), t))
			.thenApply(result -> {
					context.messageProcessingComplete(message);
					return result;
			});
	}

	protected CompletableFuture<MessageProcessingResult> execute(Collection<Message<T>> messages, MessageProcessingContext<T> context) {
		Assert.state(this.taskExecutor != null, "TaskExecutor cannot be null");
		return CompletableFuture.supplyAsync(() -> getMessageListener().onMessage(messages), this.taskExecutor)
			.thenCompose(Function.identity())
			.thenApply(theVoid -> MessageProcessingResult.successfulMessages(messages))
			.exceptionally(t -> handleListenerError(messages, t))
			.thenApply(result -> {
					messages.forEach(m -> context.messageProcessingComplete(messages));
					return result;
			});
	}

	private MessageProcessingResult handleListenerError(Collection<Message<T>> messages, Throwable t) {
		logger.error("Error in message listener.", t);
		return MessageProcessingResult.failedMessages(messages);
	}

	@Override
	public void start() {
		if (isRunning()) {
			logger.debug("Sink already running");
			return;
		}
		synchronized (this.lifecycleMonitor) {
			Assert.notNull(this.messageListener, "messageListener cannot be null");
			logger.debug("Starting sink");
			this.running = true;
		}
	}

	@Override
	public void stop() {
		if (!isRunning()) {
			logger.debug("Sink already stopped");
			return;
		}
		synchronized (this.lifecycleMonitor) {
			logger.debug("Stopping Sink");
			this.running = false;
			if (this.taskExecutor instanceof DisposableBean) {
				try {
					((DisposableBean) this.taskExecutor).destroy();
				}
				catch (Exception e) {
					throw new IllegalStateException("Error destroying TaskExecutor for sink.");
				}
			}
		}
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	public void setTaskExecutor(TaskExecutor taskExecutor) {
		Assert.notNull(taskExecutor, "taskExecutor cannot be null");
		this.taskExecutor = taskExecutor;
	}

}
