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
package io.awspring.cloud.sqs.config;

import io.awspring.cloud.messaging.support.config.AbstractMessagePollerFactory;
import io.awspring.cloud.messaging.support.listener.AsyncMessagePoller;
import io.awspring.cloud.sqs.listener.SqsMessagePoller;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessagePollerFactory extends AbstractMessagePollerFactory<String> {

	private final SqsAsyncClientSupplier sqsAsyncClientSupplier;

	public SqsMessagePollerFactory(SqsAsyncClientSupplier sqsAsyncClientSupplier) {
		Assert.notNull(sqsAsyncClientSupplier, "sqsAsyncClientSupplier cannot be null.");
		this.sqsAsyncClientSupplier = sqsAsyncClientSupplier;
	}

	@Override
	protected Collection<AsyncMessagePoller<String>> doCreateProducers(Collection<String> endpointNames) {
		SqsAsyncClient sqsAsyncClient = this.sqsAsyncClientSupplier.createNewInstance();
		return endpointNames.stream().map(name -> createSqsProducer(name, sqsAsyncClient)).collect(Collectors.toList());
	}

	private AsyncMessagePoller<String> createSqsProducer(String endpointName, SqsAsyncClient sqsAsyncClient) {
		return new SqsMessagePoller(endpointName, sqsAsyncClient);
	}
}
