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
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import io.awspring.cloud.sqs.listener.interceptor.MessageVisibilityExtenderInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.function.Supplier;

/**
 * {@link MessageListenerContainerFactory} implementation for creating
 * {@link SqsMessageListenerContainer} instances.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessageListenerContainerFactory<T>
		extends AbstractMessageListenerContainerFactory<T, SqsMessageListenerContainer<T>> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainerFactory.class);

	private final ContainerOptions containerOptions;

	private Supplier<SqsAsyncClient> sqsAsyncClientSupplier;

	public SqsMessageListenerContainerFactory() {
		this(ContainerOptions.create());
	}

	private SqsMessageListenerContainerFactory(ContainerOptions containerOptions) {
		this.containerOptions = containerOptions;
	}

	@Override
	protected SqsMessageListenerContainer<T> createContainerInstance(Endpoint endpoint) {
		logger.debug("Creating {} for endpoint {}", SqsMessageListenerContainer.class.getSimpleName(), endpoint);
		Assert.notNull(this.sqsAsyncClientSupplier, "No asyncClient set");
		SqsAsyncClient asyncClient = this.sqsAsyncClientSupplier.get();
		return new SqsMessageListenerContainer<>(asyncClient, createContainerOptions(endpoint));
	}

	protected ContainerOptions createContainerOptions(Endpoint endpoint) {
		ContainerOptions options = this.containerOptions.createCopy();
		if (endpoint instanceof SqsEndpoint) {
			SqsEndpoint sqsEndpoint = (SqsEndpoint) endpoint;
			ConfigUtils.INSTANCE
					.acceptIfNotNull(sqsEndpoint.getMinimumVisibility(), options::minTimeToProcess)
					.acceptIfNotNull(sqsEndpoint.getMaxInflightMessagesPerQueue(), options::maxInflightMessagesPerQueue)
					.acceptIfNotNull(sqsEndpoint.getPollTimeout(), options::pollTimeout)
					.acceptIfNotNull(sqsEndpoint.getMinimumVisibility(), this::addVisibilityExtender);
		}
		return options;
	}

	public ContainerOptions getContainerOptions() {
		return this.containerOptions;
	}

	public void setSqsAsyncClientSupplier(Supplier<SqsAsyncClient> sqsAsyncClientSupplier) {
		Assert.notNull(sqsAsyncClientSupplier, "sqsAsyncClientSupplier cannot be null.");
		this.sqsAsyncClientSupplier = sqsAsyncClientSupplier;
	}

	public void setSqsAsyncClient(SqsAsyncClient sqsAsyncClient) {
		Assert.notNull(sqsAsyncClient, "sqsAsyncClient cannot be null.");
		setSqsAsyncClientSupplier(() -> sqsAsyncClient);
	}

	private void addVisibilityExtender(Integer minTimeToProcess) {
		MessageVisibilityExtenderInterceptor<T> interceptor = new MessageVisibilityExtenderInterceptor<>();
		interceptor.setMinimumVisibility(minTimeToProcess);
		super.addMessageInterceptor(interceptor);
	}

}
