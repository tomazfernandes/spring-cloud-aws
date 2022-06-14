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
package io.awspring.cloud.messaging.support.config;

import io.awspring.cloud.messaging.support.MessagingUtils;
import io.awspring.cloud.messaging.support.listener.AbstractContainerOptions;
import io.awspring.cloud.messaging.support.listener.AbstractMessageListenerContainer;
import org.springframework.messaging.Message;

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
public abstract class AbstractMessageListenerContainerFactory<T, C extends AbstractMessageListenerContainer<T>, E extends AbstractEndpoint<T>>
		implements MessageListenerContainerFactory<C, E> {

	private final AbstractContainerOptions<T, ?> containerOptions;
	private final MessagePollerFactory<T> messagePollerFactory;

	protected AbstractMessageListenerContainerFactory(MessagePollerFactory<T> messagePollerFactory,
													  AbstractContainerOptions<T, ?> containerOptions) {

		this.containerOptions = containerOptions;
		this.messagePollerFactory = messagePollerFactory;
	}

	@Override
	public C create(E endpoint) {
		C container = createContainerInstance(endpoint);
		container.setId(endpoint.getId());
		container.getContainerOptions().messagePollers(this.messagePollerFactory.create(endpoint.getLogicalNames()));
		return configureContainer(container);
	}

	@Override
	public C create(Collection<String> endpointNames) {
		C container = createContainerInstance(endpointNames);
		container.getContainerOptions().messagePollers(this.messagePollerFactory.create(endpointNames));
		return configureContainer(container);
	}

	private C configureContainer(C container) {
		AbstractContainerOptions<T, ?> containerOptions = container.getContainerOptions();
		MessagingUtils.INSTANCE.acceptIfNotNull(this.containerOptions.getErrorHandler(), containerOptions::errorHandler)
			.acceptIfNotNull(this.containerOptions.getAckHandler(), containerOptions::ackHandler)
			.acceptIfNotNull(this.containerOptions.getMessageInterceptor(), containerOptions::messageInterceptor);
		initializeContainer(container);
		return container;
	}

	protected abstract C createContainerInstance(E endpoint);

	protected abstract C createContainerInstance(Collection<String> endpointNames);

	protected void initializeContainer(C container) {
	}

	protected AbstractContainerOptions<T, ?> getContainerOptions() {
		return this.containerOptions;
	}

}
