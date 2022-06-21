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
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class ContainerOptions {

	private static final int DEFAULT_SIMULTANEOUS_POLL_CALLS = 2;

	private static final int DEFAULT_MESSAGES_PER_POLL = 10;

	private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(10);

	private final Duration DEFAULT_SEMAPHORE_TIMEOUT = Duration.ofSeconds(10);

	private int maxInflightMessagesPerQueue = DEFAULT_SIMULTANEOUS_POLL_CALLS;

	private int messagesPerPoll = DEFAULT_MESSAGES_PER_POLL;

	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

	private Duration semaphoreAcquireTimeout = DEFAULT_SEMAPHORE_TIMEOUT;

	private Integer minTimeToProcess;

	public static ContainerOptions create() {
		return new ContainerOptions();
	}

	public ContainerOptions minTimeToProcess(Integer minTimeToProcess) {
		this.minTimeToProcess = minTimeToProcess;
		return this;
	}

	public Integer getMinTimeToProcess() {
		return minTimeToProcess;
	}

	public ContainerOptions maxInflightMessagesPerQueue(int maxInflightMessagesPerQueue) {
		this.maxInflightMessagesPerQueue = maxInflightMessagesPerQueue;
		return this;
	}

	public ContainerOptions semaphoreAcquireTimeout(Duration semaphoreAcquireTimeout) {
		Assert.notNull(semaphoreAcquireTimeout, "semaphoreAcquireTimeout cannot be null");
		this.semaphoreAcquireTimeout = semaphoreAcquireTimeout;
		return this;
	}

	public ContainerOptions messagesPerPoll(int messagesPerPoll) {
		this.messagesPerPoll = messagesPerPoll;
		return this;
	}

	public ContainerOptions pollTimeout(Duration pollTimeout) {
		Assert.notNull(pollTimeout, "pollTimeout cannot be null");
		this.pollTimeout = pollTimeout;
		return this;
	}

	public int getMaxInFlightMessagesPerQueue() {
		return this.maxInflightMessagesPerQueue;
	}

	public int getMessagesPerPoll() {
		return this.messagesPerPoll;
	}

	public Duration getPollTimeout() {
		return this.pollTimeout;
	}

	public Duration getSemaphoreAcquireTimeout() {
		return semaphoreAcquireTimeout;
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
