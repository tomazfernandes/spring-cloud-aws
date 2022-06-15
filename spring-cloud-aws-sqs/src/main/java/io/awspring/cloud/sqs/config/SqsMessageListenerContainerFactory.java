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
import io.awspring.cloud.sqs.listener.SqsContainerOptions;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * {@link MessageListenerContainerFactory} implementation for creating
 * {@link SqsMessageListenerContainer} instances.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessageListenerContainerFactory<T>
		extends AbstractMessageListenerContainerFactory<T, SqsMessageListenerContainer<T>, SqsEndpoint> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainerFactory.class);

	private final SqsContainerOptions sqsContainerOptions;
	private Supplier<SqsAsyncClient> sqsAsyncClientSupplier;

	public SqsMessageListenerContainerFactory() {
		this(SqsContainerOptions.create());
	}

	private SqsMessageListenerContainerFactory(SqsContainerOptions containerOptions) {
		super(containerOptions);
		this.sqsContainerOptions = containerOptions;
	}

	@Override
	protected SqsEndpoint createEndpointAdapter(Collection<String> endpointNames) {
		return SqsEndpoint.from(endpointNames).build();
	}

	@Override
	protected SqsMessageListenerContainer<T> doCreateContainerInstance(SqsEndpoint endpoint) {
		logger.debug("Creating {} for endpoint {}", SqsMessageListenerContainer.class.getSimpleName(), endpoint);
		Assert.notNull(this.sqsAsyncClientSupplier, "No asyncClient set");
		return new SqsMessageListenerContainer<>(this.sqsAsyncClientSupplier.get(),
			createContainerOptions(endpoint));
	}

	protected SqsContainerOptions createContainerOptions(SqsEndpoint endpoint) {
		SqsContainerOptions options = this.sqsContainerOptions.createCopy();
		MessagingUtils.INSTANCE
				.acceptIfNotNull(endpoint.getMinTimeToProcess(), options::minTimeToProcess)
				.acceptIfNotNull(endpoint.getSimultaneousPollsPerQueue(), options::simultaneousPolls)
				.acceptIfNotNull(endpoint.getPollTimeout(), options::pollTimeout);
		return options;
	}

	public SqsContainerOptions getContainerOptions() {
		return this.sqsContainerOptions;
	}

	public void setSqsAsyncClientSupplier(Supplier<SqsAsyncClient> sqsAsyncClientSupplier) {
		Assert.notNull(sqsAsyncClientSupplier, "sqsAsyncClientSupplier cannot be null.");
		this.sqsAsyncClientSupplier = sqsAsyncClientSupplier;
	}

	public void setSqsAsyncClient(SqsAsyncClient sqsAsyncClient) {
		Assert.notNull(sqsAsyncClient, "sqsAsyncClient cannot be null.");
		setSqsAsyncClientSupplier(() -> sqsAsyncClient);
	}

//	protected MessagePollerFactory<T> createMessagePollerFactory(SqsAsyncClientSupplier sqsAsyncClientSupplier) {
//		return new SqsMessagePollerFactory<>(sqsAsyncClientSupplier);
//	}

	//		MessagingUtils.INSTANCE.acceptBothIfNoneNull(containerOptions.getMinTimeToProcess(), container,
//				this::addVisibilityExtender);


//	private void addVisibilityExtender(Integer minTimeToProcess, SqsMessageListenerContainer container) {
//		MessageVisibilityExtenderInterceptor<String> interceptor = new MessageVisibilityExtenderInterceptor<>(
//				this.sqsContainerOptions.get);
//		interceptor.setMinTimeToProcessMessage(minTimeToProcess);
//		container.getContainerOptions().setMessageInterceptor(interceptor);
//	}

}
