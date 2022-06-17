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
package io.awspring.cloud.sqs.config;

import io.awspring.cloud.sqs.ConfigUtils;
import io.awspring.cloud.sqs.listener.AbstractMessageListenerContainer;
import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import io.awspring.cloud.sqs.listener.acknowledgement.AsyncAckHandler;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.splitter.AsyncMessageSplitter;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Base implementation for a {@link MessageListenerContainerFactory}.
 *
 * @param <T> the {@link Message} type to be consumed by the {@link AbstractMessageListenerContainer}
 * @param <C> the {@link AbstractMessageListenerContainer} type.
 * @param <E> the {@link AbstractEndpoint} type for which the {@link AbstractMessageListenerContainer} will be created.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractMessageListenerContainerFactory<T, C extends AbstractMessageListenerContainer<T>>
		implements MessageListenerContainerFactory<C> {

	private AsyncErrorHandler<T> errorHandler;

	private AsyncAckHandler<T> ackHandler;

	private AsyncMessageSplitter<T> messageSplitter;

	private final Collection<AsyncMessageInterceptor<T>> messageInterceptors = new ArrayList<>();

	private AsyncMessageListener<T> messageListener;

	private final ContainerOptions containerOptions;

	protected AbstractMessageListenerContainerFactory(ContainerOptions containerOptions) {
		this.containerOptions = containerOptions;
	}

	public void setErrorHandler(AsyncErrorHandler<T> errorHandler) {
		Assert.notNull(errorHandler, "errorHandler cannot be null");
		this.errorHandler = errorHandler;
	}

	public void setAckHandler(AsyncAckHandler<T> ackHandler) {
		Assert.notNull(ackHandler, "ackHandler cannot be null");
		this.ackHandler = ackHandler;
	}

	public void addMessageInterceptor(AsyncMessageInterceptor<T> messageInterceptor) {
		Assert.notNull(messageInterceptor, "messageInterceptor cannot be null");
		this.messageInterceptors.add(messageInterceptor);
	}

	public void addMessageInterceptors(Collection<AsyncMessageInterceptor<T>> messageInterceptors) {
		Assert.notEmpty(messageInterceptors, "messageInterceptors cannot be null");
		this.messageInterceptors.addAll(messageInterceptors);
	}

	public void setMessageSplitter(AsyncMessageSplitter<T> messageSplitter) {
		Assert.notNull(messageSplitter, "messageSplitter cannot be null");
		this.messageSplitter = messageSplitter;
	}

	public void setMessageListener(AsyncMessageListener<T> messageListener) {
		Assert.notNull(messageListener, "messageListener cannot be null");
		this.messageListener = messageListener;
	}

	@Override
	public C createContainer(Endpoint endpoint) {
		Assert.notNull(endpoint, "endpoint cannot be null");
		C container = createContainerInstance(endpoint);
		if (endpoint instanceof AbstractEndpoint) {
			configureEndpoint((AbstractEndpoint) endpoint);
		}
		endpoint.setupContainer(container);
		configureContainer(container, endpoint);
		return container;
	}

	@Override
	public C createContainer(String... endpointNames) {
		Assert.notEmpty(endpointNames, "endpointNames cannot be empty");
		return createContainer(new AbstractEndpoint(Arrays.asList(endpointNames), null, null) {
			@Override
			public void setupContainer(MessageListenerContainer container) {
			}
		});
	}

	private void configureEndpoint(AbstractEndpoint endpoint) {
		ConfigUtils.INSTANCE.acceptIfNotNull(this.messageSplitter, endpoint::setMessageSplitter);
	}

	protected void configureContainer(AbstractMessageListenerContainer<T> container, Endpoint endpoint) {
		container.setId(endpoint.getId());
		container.setQueueNames(endpoint.getLogicalNames());
		ConfigUtils.INSTANCE
			.acceptIfNotNull(this.messageSplitter, container::setMessageSplitter)
			.acceptIfNotNull(this.messageListener, container::setMessageListener)
			.acceptIfNotNull(this.errorHandler, container::setErrorHandler)
			.acceptIfNotNull(this.ackHandler, container::setAckHandler)
			.acceptIfNotNull(this.messageInterceptors, container::addMessageInterceptors);
	}

	protected abstract SqsEndpoint createEndpointAdapter(Collection<String> endpointNames);

	protected abstract C createContainerInstance(Endpoint endpoint);

}
