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
package io.awspring.cloud.sqs.listener.sink;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

/**
 * Base implementation for {@link MessageListeningSink} containing {@link SmartLifecycle} features and
 * {@link TaskExecutor} management.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractMessageListeningSink<T> implements MessageListeningSink<T>, SmartLifecycle {

	private static final int DEFAULT_CORE_SIZE = 10;

	private static final Logger logger = LoggerFactory.getLogger(AbstractMessageListeningSink.class);

	private int poolSize = DEFAULT_CORE_SIZE;

	private TaskExecutor taskExecutor;

	private final Object lifecycleMonitor = new Object();

	private volatile boolean running;

	public void setPoolSize(int coreSize) {
		this.poolSize = coreSize;
	}

	protected TaskExecutor getTaskExecutor() {
		return this.taskExecutor;
	}

	@Override
	public Collection<CompletableFuture<Void>> emit(Collection<Message<T>> messages,
			AsyncMessageListener<T> messageListener) {
		Assert.notNull(messages, "messages cannot be null");
		Assert.notNull(messageListener, "messageListener cannot be null");
		if (!isRunning()) {
			logger.debug("Sink not running, returning");
			return returnVoidFutures(messages);
		}
		return doEmit(messages, messageListener);
	}

	protected List<CompletableFuture<Void>> returnVoidFutures(Collection<Message<T>> messages) {
		return messages.stream().map(msg -> CompletableFuture.<Void> completedFuture(null))
				.collect(Collectors.toList());
	}

	protected abstract Collection<CompletableFuture<Void>> doEmit(Collection<Message<T>> messages,
			AsyncMessageListener<T> messageListener);

	@Override
	public void start() {
		if (isRunning()) {
			logger.debug("Sink already running");
			return;
		}
		synchronized (this.lifecycleMonitor) {
			logger.debug("Starting Sink");
			this.taskExecutor = createTaskExecutor();
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

	private TaskExecutor createTaskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setMaxPoolSize(this.poolSize);
		executor.setCorePoolSize(this.poolSize);
		executor.setThreadNamePrefix(this.getClass().getSimpleName().toLowerCase() + "-");
		executor.afterPropertiesSet();
		return executor;
	}

}
