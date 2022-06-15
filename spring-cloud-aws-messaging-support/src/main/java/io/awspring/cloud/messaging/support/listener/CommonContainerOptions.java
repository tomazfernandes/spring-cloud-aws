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
package io.awspring.cloud.messaging.support.listener;


import java.time.Duration;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class CommonContainerOptions<O extends CommonContainerOptions<?>> {

	private static final int DEFAULT_SIMULTANEOUS_POLL_CALLS = 2;

	private static final int DEFAULT_MESSAGES_PER_POLL = 10;

	private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(10);

	private int simultaneousPolls = DEFAULT_SIMULTANEOUS_POLL_CALLS;

	private int messagesPerPoll = DEFAULT_MESSAGES_PER_POLL;

	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

	@SuppressWarnings("unchecked")
	private O self() {
		return (O) this;
	}

	public O simultaneousPolls(int simultaneousPollCalls) {
		this.simultaneousPolls = simultaneousPollCalls;
		return self();
	}

	public O messagesPerPoll(int messagesPerPoll) {
		this.messagesPerPoll = messagesPerPoll;
		return self();
	}

	public O pollTimeout(Duration pollTimeout) {
		this.pollTimeout = pollTimeout;
		return self();
	}

	public int getSimultaneousPolls() {
		return this.simultaneousPolls;
	}

	public int getMessagesPerPoll() {
		return this.messagesPerPoll;
	}

	public Duration getPollTimeout() {
		return this.pollTimeout;
	}

	/**
	 * Creates a shallow copy of these options.
	 * @return the copy.
	 */
	public O createCopy() {
		return doCreateCopy();
	}

	protected abstract O doCreateCopy();

}
