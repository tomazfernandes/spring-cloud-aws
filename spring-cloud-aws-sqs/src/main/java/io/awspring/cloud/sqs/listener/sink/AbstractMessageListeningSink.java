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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

/**
 * Base implementation for {@link MessageSink} containing {@link SmartLifecycle} features and
 * {@link TaskExecutor} management.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractMessageListeningSink<T> implements MessageListeningMessageSink<T>,
	DisposableBean, SmartInitializingSingleton {

	private static final int DEFAULT_CORE_SIZE = 10;

	private static final Logger logger = LoggerFactory.getLogger(AbstractMessageListeningSink.class);

	private int poolSize = DEFAULT_CORE_SIZE;

	private TaskExecutor taskExecutor;

	private AsyncMessageListener<T> messageListener;

	public void setPoolSize(int coreSize) {
		this.poolSize = coreSize;
	}

	protected TaskExecutor getTaskExecutor() {
		return this.taskExecutor;
	}

	@Override
	public void setMessageListener(AsyncMessageListener<T> messageListener) {
		Assert.notNull(messageListener, "messageListener cannot be null");
		Assert.isNull(this.messageListener, "messageListener already set");
		this.messageListener = messageListener;
	}

	protected AsyncMessageListener<T> getMessageListener() {
		return this.messageListener;
	}

	@Override
	public Collection<CompletableFuture<Void>> emit(Collection<Message<T>> messages) {
		return doEmit(messages);
	}

	protected abstract Collection<CompletableFuture<Void>> doEmit(Collection<Message<T>> messages);

	@Override
	public void afterSingletonsInstantiated() {
		Assert.notNull(this.messageListener, "No messageListener set.");
		this.taskExecutor = createTaskExecutor();
	}

	@Override
	public void destroy() throws Exception {
		if (this.taskExecutor instanceof DisposableBean) {
			((DisposableBean) this.taskExecutor).destroy();
		}
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
