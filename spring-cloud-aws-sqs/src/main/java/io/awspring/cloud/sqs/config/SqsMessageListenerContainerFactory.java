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

import io.awspring.cloud.sqs.MessagingUtils;
import io.awspring.cloud.sqs.listener.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import io.awspring.cloud.sqs.listener.SqsContainerOptions;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import io.awspring.cloud.sqs.listener.acknowledgement.AsyncAckHandler;
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
		extends AbstractMessageListenerContainerFactory<T, SqsMessageListenerContainer<T>> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainerFactory.class);

	private final SqsContainerOptions sqsContainerOptions;

	private Supplier<SqsAsyncClient> sqsAsyncClientSupplier;

	private AsyncErrorHandler<T> errorHandler;

	private AsyncAckHandler<T> ackHandler;

	private AsyncMessageInterceptor<T> messageInterceptor;

	private AsyncMessageListener<T> messageListener;

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
	protected SqsMessageListenerContainer<T> createContainerInstance(Endpoint endpoint) {
		logger.debug("Creating {} for endpoint {}", SqsMessageListenerContainer.class.getSimpleName(), endpoint);
		Assert.notNull(this.sqsAsyncClientSupplier, "No asyncClient set");
		return new SqsMessageListenerContainer<>(this.sqsAsyncClientSupplier.get(),
			createContainerOptions(endpoint));
	}

	protected SqsContainerOptions createContainerOptions(Endpoint endpoint) {
		Assert.isInstanceOf(SqsEndpoint.class, endpoint, "Endpoint must be an instance of " + SqsEndpoint.class.getSimpleName());
		SqsEndpoint sqsEndpoint = (SqsEndpoint) endpoint;
		SqsContainerOptions options = this.sqsContainerOptions.createCopy();
		MessagingUtils.INSTANCE
				.acceptIfNotNull(sqsEndpoint.getMinTimeToProcess(), options::minTimeToProcess)
				.acceptIfNotNull(sqsEndpoint.getSimultaneousPollsPerQueue(), options::simultaneousPolls)
				.acceptIfNotNull(sqsEndpoint.getPollTimeout(), options::pollTimeout);
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

	protected void configureContainer(SqsMessageListenerContainer<T> container, Endpoint endpoint) {
		container.setId(endpoint.getId());
		container.setQueueNames(endpoint.getLogicalNames());
		MessagingUtils.INSTANCE
			.acceptIfNotNull(this.messageListener, container::setMessageListener)
			.acceptIfNotNull(this.errorHandler, container::setErrorHandler)
			.acceptIfNotNull(this.ackHandler, container::setAckHandler)
			.acceptIfNotNull(this.messageInterceptor, container::setMessageInterceptor);
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
