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

import io.awspring.cloud.messaging.support.MessagingUtils;
import io.awspring.cloud.messaging.support.listener.AbstractMessageListenerContainer;
import io.awspring.cloud.messaging.support.listener.AsyncErrorHandler;
import io.awspring.cloud.messaging.support.listener.AsyncMessageInterceptor;
import io.awspring.cloud.messaging.support.listener.AsyncMessageListener;
import io.awspring.cloud.messaging.support.listener.CommonContainerOptions;
import io.awspring.cloud.messaging.support.listener.acknowledgement.AsyncAckHandler;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

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
public abstract class AbstractMessageListenerContainerFactory<T, C extends AbstractMessageListenerContainer<T>, E extends AbstractEndpoint>
		implements MessageListenerContainerFactory<C, E> {

	private AsyncErrorHandler<T> errorHandler;

	private AsyncAckHandler<T> ackHandler;

	private AsyncMessageInterceptor<T> messageInterceptor;

	private AsyncMessageListener<T> messageListener;

	private final CommonContainerOptions<?> containerOptions;

	protected AbstractMessageListenerContainerFactory(CommonContainerOptions<?> containerOptions) {
		this.containerOptions = containerOptions;
	}

	@Override
	public C createContainerInstance(E endpoint) {
		Assert.notNull(endpoint, "endpoint cannot be null");
		C container = doCreateContainerInstance(endpoint);
		configureContainer(container, endpoint);
		return container;
	}

	@Override
	public C createContainerInstance(String... endpointNames) {
		Assert.notEmpty(endpointNames, "endpointNames cannot be empty");
		return createContainerInstance(createEndpointAdapter(Arrays.asList(endpointNames)));
	}

	protected abstract E createEndpointAdapter(Collection<String> endpointNames);

	public void setErrorHandler(AsyncErrorHandler<T> errorHandler) {
		Assert.notNull(errorHandler, "errorHandler cannot be null");
		this.errorHandler = errorHandler;
	}

	public void setAckHandler(AsyncAckHandler<T> ackHandler) {
		Assert.notNull(ackHandler, "ackHandler cannot be null");
		this.ackHandler = ackHandler;
	}

	public void setMessageInterceptor(AsyncMessageInterceptor<T> messageInterceptor) {
		Assert.notNull(messageInterceptor, "messageInterceptor cannot be null");
		this.messageInterceptor = messageInterceptor;
	}

	public void setMessageListener(AsyncMessageListener<T> messageListener) {
		Assert.notNull(messageListener, "messageListener cannot be null");
		this.messageListener = messageListener;
	}

	protected void configureContainer(C container, E endpoint) {
		container.setId(endpoint.getId());
		container.setAssignments(endpoint.getLogicalNames());
		MessagingUtils.INSTANCE
			.acceptIfNotNull(this.messageListener, container::setMessageListener)
			.acceptIfNotNull(this.errorHandler, container::setErrorHandler)
			.acceptIfNotNull(this.ackHandler, container::setAckHandler)
			.acceptIfNotNull(this.messageInterceptor, container::setMessageInterceptor);
	}

	protected abstract C doCreateContainerInstance(E endpoint);

}
