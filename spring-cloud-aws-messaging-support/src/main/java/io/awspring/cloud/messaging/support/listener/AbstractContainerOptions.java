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

import io.awspring.cloud.messaging.support.listener.acknowledgement.AsyncAckHandler;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractContainerOptions<T, O extends AbstractContainerOptions<T, ?>> implements ContainerOptions<T> {

	private static final int DEFAULT_SIMULTANEOUS_POLL_CALLS = 2;

	private static final int DEFAULT_MESSAGES_PER_POLL = 10;

	private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(10);

	private int simultaneousPolls = DEFAULT_SIMULTANEOUS_POLL_CALLS;

	private int messagesPerPoll = DEFAULT_MESSAGES_PER_POLL;

	private Duration pollTimeout = DEFAULT_POLL_TIMEOUT;

	private AsyncErrorHandler<T> errorHandler;

	private AsyncAckHandler<T> ackHandler;

	private AsyncMessageInterceptor<T> messageInterceptor;

	private AsyncMessageListener<T> messageListener;

	private Collection<AsyncMessagePoller<T>> messagePollers = new ArrayList<>();

	@SuppressWarnings("unchecked")
	private O self() {
		return (O) this;
	}

	public O errorHandler(AsyncErrorHandler<T> errorHandler) {
		Assert.notNull(errorHandler, "errorHandler cannot be null");
		this.errorHandler = errorHandler;
		return self();
	}

	public O ackHandler(AsyncAckHandler<T> ackHandler) {
		Assert.notNull(ackHandler, "ackHandler cannot be null");
		this.ackHandler = ackHandler;
		return self();
	}

	public O messageInterceptor(AsyncMessageInterceptor<T> messageInterceptor) {
		Assert.notNull(messageInterceptor, "messageInterceptor cannot be null");
		this.messageInterceptor = messageInterceptor;
		return self();
	}

	public O messageListener(AsyncMessageListener<T> messageListener) {
		Assert.notNull(messageListener, "messageListener cannot be null");
		this.messageListener = messageListener;
		return self();
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

	public O messagePollers(Collection<AsyncMessagePoller<T>> asyncMessagePollers) {
		this.messagePollers = asyncMessagePollers;
		return self();
	}

	public O messagePoller(AsyncMessagePoller<T> asyncMessagePoller) {
		this.messagePollers = Collections.singletonList(asyncMessagePoller);
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

	public AsyncMessageListener<T> getMessageListener() {
		return this.messageListener;
	}

	public Collection<AsyncMessagePoller<T>> getMessagePollers() {
		return this.messagePollers;
	}

	public AsyncAckHandler<T> getAckHandler() {
		return this.ackHandler;
	}

	public AsyncErrorHandler<T> getErrorHandler() {
		return this.errorHandler;
	}

	public AsyncMessageInterceptor<T> getMessageInterceptor() {
		return messageInterceptor;
	}

	O createCopy() {
		return doCreateCopy();
	}

	protected abstract O doCreateCopy();

	public void setSimultaneousPolls(int simultaneousPolls) {
		this.simultaneousPolls = simultaneousPolls;
	}

	public void setMessagesPerPoll(int messagesPerPoll) {
		this.messagesPerPoll = messagesPerPoll;
	}

	public void setPollTimeout(Duration pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	public void setErrorHandler(AsyncErrorHandler<T> errorHandler) {
		this.errorHandler = errorHandler;
	}

	public void setAckHandler(AsyncAckHandler<T> ackHandler) {
		this.ackHandler = ackHandler;
	}

	public void setMessageInterceptor(AsyncMessageInterceptor<T> messageInterceptor) {
		this.messageInterceptor = messageInterceptor;
	}

	public void setMessageListener(AsyncMessageListener<T> messageListener) {
		this.messageListener = messageListener;
	}

	public void setMessagePollers(Collection<AsyncMessagePoller<T>> messagePollers) {
		this.messagePollers = messagePollers;
	}
}
