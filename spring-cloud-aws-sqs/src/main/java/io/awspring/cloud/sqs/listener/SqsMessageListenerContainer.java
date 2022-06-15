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
package io.awspring.cloud.sqs.listener;

import io.awspring.cloud.messaging.support.listener.AbstractMessageListenerContainer;
import io.awspring.cloud.messaging.support.listener.AsyncMessagePoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessageListenerContainer<T> extends AbstractMessageListenerContainer<T> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainer.class);

	private final SqsAsyncClient asyncClient;

	public SqsMessageListenerContainer(SqsAsyncClient asyncClient, SqsContainerOptions options) {
		super(options);
		this.asyncClient = asyncClient;
	}

	@Override
	protected Collection<AsyncMessagePoller<T>> doCreateMessagePollers(Collection<String> endpointNames) {
		return endpointNames.stream()
			.map(name -> new SqsMessagePoller<T>(name, this.asyncClient))
			.collect(Collectors.toList());
	}

	public void setQueueNames(Collection<String> queueNames) {
		super.setAssignments(queueNames);
	}

	public void setQueueNames(String... queueNames) {
		super.setAssignments(Arrays.asList(queueNames));
	}
}
