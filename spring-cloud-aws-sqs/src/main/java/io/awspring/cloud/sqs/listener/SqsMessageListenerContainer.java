/*
 * Copyright 2022 the original author or authors.
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
package io.awspring.cloud.sqs.listener;

import io.awspring.cloud.sqs.ConfigUtils;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.source.MessageSource;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * {@link MessageListenerContainer} implementation for SQS queues.
 *
 * Components and {@link ContainerOptions} can be changed at runtime and such changes will be valid upon container
 * restart.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessageListenerContainer<T> extends AbstractPipelineMessageListenerContainer<T> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainer.class);

	private final SqsAsyncClient sqsAsyncClient;

	public SqsMessageListenerContainer(SqsAsyncClient sqsAsyncClient, ContainerOptions options) {
		super(options);
		this.sqsAsyncClient = sqsAsyncClient;
	}

	@Override
	protected ContainerComponentFactory<T> createComponentFactory() {
		Assert.isTrue(getQueueNames().stream().map(this::isFifoQueue).distinct().count() == 1,
			"The container must contain either all FIFO or all Standard queues.");
		return isFifoQueue(getQueueNames().iterator().next())
			? new FifoSqsComponentFactory<>()
			: new StandardSqsComponentFactory<>();
	}

	private boolean isFifoQueue(String name) {
		return name.endsWith(".fifo");
	}

	@Override
	protected void doConfigureMessageSources(Collection<MessageSource<T>> messageSources) {
		ConfigUtils.INSTANCE
			.acceptManyIfInstance(messageSources, SqsAsyncClientAware.class, asca -> asca.setSqsAsyncClient(this.sqsAsyncClient));
	}

	@Override
	protected void doConfigureMessageSink(MessageSink<T> messageSink) {
		ConfigUtils.INSTANCE
			.acceptIfInstance(messageSink, SqsAsyncClientAware.class, asca -> asca.setSqsAsyncClient(this.sqsAsyncClient));
	}

}
