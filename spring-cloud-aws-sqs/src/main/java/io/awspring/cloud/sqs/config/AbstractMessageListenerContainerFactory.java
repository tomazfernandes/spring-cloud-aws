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

import io.awspring.cloud.sqs.listener.AbstractMessageListenerContainer;
import io.awspring.cloud.sqs.listener.CommonContainerOptions;
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
public abstract class AbstractMessageListenerContainerFactory<T, C extends AbstractMessageListenerContainer<T>>
		implements MessageListenerContainerFactory<C> {

	private final CommonContainerOptions<?> containerOptions;

	protected AbstractMessageListenerContainerFactory(CommonContainerOptions<?> containerOptions) {
		this.containerOptions = containerOptions;
	}

	@Override
	public C createContainer(Endpoint endpoint) {
		Assert.notNull(endpoint, "endpoint cannot be null");
		C container = createContainerInstance(endpoint);
		configureContainer(container, endpoint);
		return container;
	}

	@Override
	public C createContainer(String... endpointNames) {
		Assert.notEmpty(endpointNames, "endpointNames cannot be empty");
		return createContainer(createEndpointAdapter(Arrays.asList(endpointNames)));
	}

	protected abstract void configureContainer(C container, Endpoint endpoint);

	protected abstract SqsEndpoint createEndpointAdapter(Collection<String> endpointNames);

	protected abstract C createContainerInstance(Endpoint endpoint);

}
