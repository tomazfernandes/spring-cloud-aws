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


import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import java.time.Duration;

/**
 * Contains the options to be used by the {@link MessageListenerContainer} at runtime.
 * Changes will be reflected upon container restart.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class ContainerOptions {

	private static final int DEFAULT_MAX_INFLIGHT_MSG_PER_QUEUE = 10;

	private static final int DEFAULT_MESSAGES_PER_POLL = 10;

	private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(10);

	private static final Duration DEFAULT_SEMAPHORE_TIMEOUT = Duration.ofSeconds(10);

	private int maxInflightMessagesPerQueue = DEFAULT_MAX_INFLIGHT_MSG_PER_QUEUE;

	private int messagesPerPoll = DEFAULT_MESSAGES_PER_POLL;

	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

	private Duration semaphoreAcquireTimeout = DEFAULT_SEMAPHORE_TIMEOUT;

	public static ContainerOptions create() {
		return new ContainerOptions();
	}

	/**
	 * Set the maximum allowed number of inflight messages for each queue.
	 * @return this instance.
	 */
	public ContainerOptions maxInflightMessagesPerQueue(int maxInflightMessagesPerQueue) {
		this.maxInflightMessagesPerQueue = maxInflightMessagesPerQueue;
		return this;
	}

	/**
	 * Set the maximum time the polling thread should wait for permits.
	 * @param semaphoreAcquireTimeout the timeout.
	 * @return this instance.
	 */
	public ContainerOptions semaphoreAcquireTimeout(Duration semaphoreAcquireTimeout) {
		Assert.notNull(semaphoreAcquireTimeout, "semaphoreAcquireTimeout cannot be null");
		this.semaphoreAcquireTimeout = semaphoreAcquireTimeout;
		return this;
	}

	/**
	 * Set the number of messages that should be returned per poll.
	 * @param messagesPerPoll the number of messages.
	 * @return this instance.
	 */
	public ContainerOptions messagesPerPoll(int messagesPerPoll) {
		this.messagesPerPoll = messagesPerPoll;
		return this;
	}

	/**
	 * Set the timeout for polling messages for this endpoint.
	 * @param pollTimeout the poll timeout.
	 * @return this instance.
	 */
	public ContainerOptions pollTimeout(Duration pollTimeout) {
		Assert.notNull(pollTimeout, "pollTimeout cannot be null");
		this.pollTimeout = pollTimeout;
		return this;
	}

	/**
	 * Return the maximum allowed number of inflight messages for each queue.
	 * @return the number.
	 */
	public int getMaxInFlightMessagesPerQueue() {
		return this.maxInflightMessagesPerQueue;
	}

	/**
	 * Return the number of messages that should be returned per poll.
	 * @return the number.
	 */
	public int getMessagesPerPoll() {
		return this.messagesPerPoll;
	}

	/**
	 * Return the timeout for polling messages for this endpoint.
	 * @return the timeout duration.
	 */
	public Duration getPollTimeout() {
		return this.pollTimeout;
	}

	/**
	 * Return the maximum time the polling thread should wait for permits.
	 * @return the timeout.
	 */
	public Duration getSemaphoreAcquireTimeout() {
		return this.semaphoreAcquireTimeout;
	}

	/**
	 * Creates a shallow copy of these options.
	 * @return the copy.
	 */
	public ContainerOptions createCopy() {
		ContainerOptions newCopy = new ContainerOptions();
		ReflectionUtils.shallowCopyFieldState(this, newCopy);
		return newCopy;
	}
}
