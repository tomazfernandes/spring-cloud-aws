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
package io.awspring.cloud.sqs.listener.splitter;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Base implementation for {@link AsyncMessageSplitter} containing {@link SmartLifecycle} features and
 * {@link TaskExecutor} management.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractMessageSplitter<T> implements AsyncMessageSplitter<T>, SmartLifecycle {

	private static final int DEFAULT_CORE_SIZE = 10;

	private volatile boolean isRunning;

	private int coreSize = DEFAULT_CORE_SIZE;

	private TaskExecutor taskExecutor;

	private final Object lifecycleMonitor = new Object();

	public void setCoreSize(int coreSize) {
		this.coreSize = coreSize;
	}

	protected TaskExecutor getTaskExecutor() {
		return this.taskExecutor;
	}

	@Override
	public void start() {
		synchronized (this.lifecycleMonitor) {
			if (this.isRunning) {
				return;
			}
			this.taskExecutor = createTaskExecutor();
			this.isRunning = true;
		}
	}

	@Override
	public Collection<CompletableFuture<Void>> splitAndProcess(Collection<Message<T>> messages,
			Function<Message<T>, CompletableFuture<Void>> processingPipeline) {
		if (!this.isRunning) {
			return returnCompletedVoidFutures(messages);
		}
		return doSplitAndProcessMessages(messages, processingPipeline);
	}

	protected Collection<CompletableFuture<Void>> returnCompletedVoidFutures(Collection<Message<T>> messages) {
		return messages.stream().map(msg -> CompletableFuture.<Void> completedFuture(null))
				.collect(Collectors.toList());
	}

	protected abstract Collection<CompletableFuture<Void>> doSplitAndProcessMessages(Collection<Message<T>> messages,
			Function<Message<T>, CompletableFuture<Void>> processingPipeline);

	@Override
	public void stop() {
		synchronized (this.lifecycleMonitor) {
			if (!this.isRunning) {
				return;
			}
			this.isRunning = false;
			if (this.taskExecutor instanceof DisposableBean) {
				try {
					((DisposableBean) this.taskExecutor).destroy();
				}
				catch (Exception e) {
					throw new IllegalStateException("Error shutting down executor", e);
				}
			}
		}
	}

	@Override
	public boolean isRunning() {
		return this.isRunning;
	}

	protected TaskExecutor createTaskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setMaxPoolSize(this.coreSize);
		executor.setCorePoolSize(this.coreSize);
		executor.setThreadNamePrefix(this.getClass().getSimpleName().toLowerCase() + "-");
		executor.afterPropertiesSet();
		return executor;
	}

}
