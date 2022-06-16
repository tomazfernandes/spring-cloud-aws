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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

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

	private Collection<String> queueNames;

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
