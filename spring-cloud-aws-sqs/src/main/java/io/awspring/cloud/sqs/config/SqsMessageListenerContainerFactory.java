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
import io.awspring.cloud.messaging.support.config.AbstractMessageListenerContainerFactory;
import io.awspring.cloud.messaging.support.config.MessageListenerContainerFactory;
import io.awspring.cloud.messaging.support.config.MessagePollerFactory;
import io.awspring.cloud.sqs.listener.SqsContainerOptions;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;
import software.amazon.awssdk.core.internal.http.AmazonAsyncHttpClient;

import java.util.Collection;

/**
 * {@link MessageListenerContainerFactory} implementation for creating
 * {@link SqsMessageListenerContainer} instances.
 * It is typed as String since {@link AmazonAsyncHttpClient} returns strings
 * rather than POJOs.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessageListenerContainerFactory
		extends AbstractMessageListenerContainerFactory<String, SqsMessageListenerContainer, SqsEndpoint> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainerFactory.class);

	private final SqsContainerOptions sqsContainerOptions;

	public SqsMessageListenerContainerFactory(SqsMessagePollerFactory pollerFactory) {
		this(pollerFactory, SqsContainerOptions.create());
	}

	public SqsMessageListenerContainerFactory(SqsMessagePollerFactory pollerFactory, SqsContainerOptions containerOptions) {
		super(pollerFactory, containerOptions);
		this.sqsContainerOptions = containerOptions;
	}

	@Override
	protected SqsMessageListenerContainer createContainerInstance(SqsEndpoint endpoint) {
		SqsContainerOptions containerOptions = createContainerOptions(endpoint);
		SqsMessageListenerContainer container = new SqsMessageListenerContainer(containerOptions);
//		MessagingUtils.INSTANCE.acceptBothIfNoneNull(containerOptions.getMinTimeToProcess(), container,
//				this::addVisibilityExtender);
		return container;
	}

	@Override
	protected SqsMessageListenerContainer createContainerInstance(Collection<String> endpointNames) {
		return createContainerInstance(SqsEndpoint.from(endpointNames).build());
	}

//	private void addVisibilityExtender(Integer minTimeToProcess, SqsMessageListenerContainer container) {
//		MessageVisibilityExtenderInterceptor<String> interceptor = new MessageVisibilityExtenderInterceptor<>(
//				this.sqsContainerOptions.get);
//		interceptor.setMinTimeToProcessMessage(minTimeToProcess);
//		container.getContainerOptions().setMessageInterceptor(interceptor);
//	}

	protected SqsContainerOptions createContainerOptions(SqsEndpoint endpoint) {
		SqsContainerOptions options = SqsContainerOptions.create();
		ReflectionUtils.shallowCopyFieldState(this.sqsContainerOptions, options);
		MessagingUtils.INSTANCE
				.acceptIfNotNull(endpoint.getMinTimeToProcess(), options::minTimeToProcess)
				.acceptIfNotNull(endpoint.getSimultaneousPollsPerQueue(), options::simultaneousPolls)
				.acceptIfNotNull(endpoint.getPollTimeout(), options::pollTimeout);
		return options;
	}
}
