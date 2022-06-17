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

import io.awspring.cloud.sqs.listener.acknowledgement.AsyncAckHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.OnSuccessAckHandler;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.errorhandler.LoggingErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.poller.AsyncMessagePoller;
import io.awspring.cloud.sqs.listener.splitter.AsyncMessageSplitter;
import io.awspring.cloud.sqs.listener.splitter.FanOutSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractMessageListenerContainer<T> implements MessageListenerContainer<T> {

	private static final Logger logger = LoggerFactory.getLogger(AbstractMessageListenerContainer.class);

	private final LoggingErrorHandler<T> DEFAULT_ERROR_HANDLER = new LoggingErrorHandler<>();

	private final OnSuccessAckHandler<T> DEFAULT_ACK_HANDLER = new OnSuccessAckHandler<>();

	private final FanOutSplitter<T> DEFAULT_MESSAGE_SPLITTER = new FanOutSplitter<>();

	private final Object lifecycleMonitor = new Object();

	private final ContainerOptions containerOptions;

	private volatile boolean isRunning;

	private String id;

	private Collection<String> queueNames;

	private Collection<AsyncMessagePoller<T>> messagePollers;

	private AsyncMessageListener<T> messageListener;

	private AsyncErrorHandler<T> errorHandler = DEFAULT_ERROR_HANDLER;

	private AsyncAckHandler<T> ackHandler = DEFAULT_ACK_HANDLER;

	private AsyncMessageSplitter<T> splitter = DEFAULT_MESSAGE_SPLITTER;

	private final Collection<AsyncMessageInterceptor<T>> messageInterceptors = new ArrayList<>();

	protected AbstractMessageListenerContainer(ContainerOptions options) {
		Assert.notNull(options, "options cannot be null");
		this.containerOptions = options;
	}

	public void setId(String id) {
		Assert.state(this.id == null, () -> "id already set for container " + this.id);
		this.id = id;
	}

	public void setErrorHandler(AsyncErrorHandler<T> errorHandler) {
		Assert.notNull(errorHandler, "errorHandler cannot be null");
		this.errorHandler = errorHandler;
	}

	public void setAckHandler(AsyncAckHandler<T> ackHandler) {
		Assert.notNull(ackHandler, "ackHandler cannot be null");
		this.ackHandler = ackHandler;
	}

	public void addMessageInterceptors(Collection<AsyncMessageInterceptor<T>> messageInterceptors) {
		Assert.notNull(messageInterceptors, "messageInterceptors cannot be null");
		this.messageInterceptors.addAll(messageInterceptors);
	}

	@Override
	public void setMessageSplitter(AsyncMessageSplitter<T> splitter) {
		Assert.notNull(splitter, "splitter cannot be null");
		this.splitter = splitter;
	}

	@Override
	public void setMessageListener(AsyncMessageListener<T> asyncMessageListener) {
		this.messageListener = asyncMessageListener;
	}

	protected Collection<AsyncMessagePoller<T>> getMessagePollers() {
		return this.messagePollers;
	}

	public AsyncMessageListener<T> getMessageListener() {
		return this.messageListener;
	}

	public AsyncErrorHandler<T> getErrorHandler() {
		return this.errorHandler;
	}

	public AsyncAckHandler<T> getAckHandler() {
		return this.ackHandler;
	}

	public AsyncMessageSplitter<T> getSplitter() {
		return this.splitter;
	}

	public Collection<AsyncMessageInterceptor<T>> getMessageInterceptors() {
		return this.messageInterceptors;
	}

	@Override
	public String getId() {
		return this.id;
	}

	public void setQueueNames(Collection<String> queueNames) {
		this.queueNames = queueNames;
	}

	public void setQueueNames(String... queueNames) {
		setQueueNames(Arrays.asList(queueNames));
	}

	@Override
	public boolean isRunning() {
		return this.isRunning;
	}

	@Override
	public void start() {
		if (this.isRunning) {
			return;
		}
		synchronized (this.lifecycleMonitor) {
			this.isRunning = true;
			if (this.id == null) {
				this.id = resolveContainerId();
			}
			Assert.notEmpty(this.queueNames, "No endpoint names set");
			doStart();
		}
		logger.debug("Container started {}", this.id);
	}

	private String resolveContainerId() {
		return "io.awspring.cloud.sqs.sqsListenerEndpointContainer#" +
			this.queueNames.stream()
				.findFirst()
				.orElseGet(() -> UUID.randomUUID().toString());
	}

	protected void doStart() {
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
		}
		logger.debug("Container stopped {}", this.id);
	}

	protected void doStop() {
	}

	protected Collection<String> getQueueNames() {
		return this.queueNames;
	}
}
