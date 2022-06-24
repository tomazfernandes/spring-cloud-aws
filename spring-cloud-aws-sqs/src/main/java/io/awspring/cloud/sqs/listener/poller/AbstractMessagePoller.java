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
package io.awspring.cloud.sqs.listener.poller;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.messaging.Message;

/**
 * Base implementation of {@link AsyncMessagePoller} with {@link SmartLifecycle} capabilities.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractMessagePoller<T> implements AsyncMessagePoller<T>, SmartLifecycle {

	private static final Logger logger = LoggerFactory.getLogger(AbstractMessagePoller.class);

	private final String logicalEndpointName;

	private volatile boolean running;

	private final Object lifecycleMonitor = new Object();

	protected AbstractMessagePoller(String logicalEndpointName) {
		this.logicalEndpointName = logicalEndpointName;
	}

	@Override
	public CompletableFuture<Collection<Message<T>>> poll(int numberOfMessages, Duration timeout) {
		if (!this.isRunning()) {
			logger.debug("Producer not running, returning.");
			return CompletableFuture.completedFuture(null);
		}
		return doPollForMessages(numberOfMessages, timeout);
	}

	protected abstract CompletableFuture<Collection<Message<T>>> doPollForMessages(int numberOfMessages,
			Duration timeout);

	@Override
	public void start() {
		synchronized (this.lifecycleMonitor) {
			logger.debug("Starting SqsMessageProducer for {}", this.logicalEndpointName);
			this.running = true;
			doStart();
		}
	}

	protected void doStart() {
	}

	@Override
	public void stop() {
		synchronized (this.lifecycleMonitor) {
			logger.debug("Stopping SqsMessageProducer for {}", this.logicalEndpointName);
			this.running = false;
			doStop();
		}
	}

	protected void doStop() {
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	public String getLogicalEndpointName() {
		return this.logicalEndpointName;
	}
}
