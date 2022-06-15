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

import io.awspring.cloud.messaging.support.config.AbstractEndpoint;
import io.awspring.cloud.messaging.support.config.Endpoint;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.QueueAttributes;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

/**
 * {@link Endpoint} implementation for SQS endpoints.
 *
 * Contains properties that should be mapped from {@link SqsListener @SqsListener}
 * annotations.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsEndpoint extends AbstractEndpoint {

	private final Integer simultaneousPollsPerQueue;

	private final Integer pollTimeoutSeconds;

	private final Integer minTimeToProcess;

	private final Map<String, QueueAttributes> queuesAttributes;

	private final Boolean isAsync;

	private SqsEndpoint(Collection<String> logicalEndpointNames, String listenerContainerFactoryName,
						Integer simultaneousPollsPerQueue, Integer pollTimeoutSeconds, Integer minTimeToProcess,
						Map<String, QueueAttributes> queueAttributesMap, Boolean isAsync, String id) {
		super(logicalEndpointNames, listenerContainerFactoryName, id);
		this.queuesAttributes = queueAttributesMap;
		this.simultaneousPollsPerQueue = simultaneousPollsPerQueue;
		this.pollTimeoutSeconds = pollTimeoutSeconds;
		this.minTimeToProcess = minTimeToProcess;
		this.isAsync = isAsync;
	}

	public static <T> SqsEndpointBuilder<T> from(Collection<String> logicalEndpointNames) {
		return new SqsEndpointBuilder<>(logicalEndpointNames);
	}

	public Integer getSimultaneousPollsPerQueue() {
		return this.simultaneousPollsPerQueue;
	}

	public Integer getPollTimeoutSeconds() {
		return this.pollTimeoutSeconds;
	}

	public Duration getPollTimeout() {
		return this.pollTimeoutSeconds != null ? Duration.ofSeconds(this.pollTimeoutSeconds) : null;
	}

	public Integer getMinTimeToProcess() {
		return this.minTimeToProcess;
	}

	public QueueAttributes getAttributesFor(String queueName) {
		return this.queuesAttributes.get(queueName);
	}

	public Map<String, QueueAttributes> getQueuesAttributes() {
		return this.queuesAttributes;
	}

	public boolean isAsync() {
		return this.isAsync;
	}

	public static class SqsEndpointBuilder<T> {

		private final Collection<String> logicalEndpointNames;

		private Integer simultaneousPollsPerQueue;

		private Integer pollTimeoutSeconds;

		private String factoryName;

		private Integer minTimeToProcess;

		private Map<String, QueueAttributes> queuesAttributes;

		private Boolean async;

		private String id;

		public SqsEndpointBuilder(Collection<String> logicalEndpointNames) {
			this.logicalEndpointNames = logicalEndpointNames;
		}

		public SqsEndpointBuilder<T> factoryBeanName(String factoryName) {
			this.factoryName = factoryName;
			return this;
		}

		public SqsEndpointBuilder<T> simultaneousPollsPerQueue(Integer simultaneousPollsPerQueue) {
			this.simultaneousPollsPerQueue = simultaneousPollsPerQueue;
			return this;
		}

		public SqsEndpointBuilder<T> pollTimeoutSeconds(Integer pollTimeoutSeconds) {
			this.pollTimeoutSeconds = pollTimeoutSeconds;
			return this;
		}

		public SqsEndpointBuilder<T> minTimeToProcess(Integer minTimeToProcess) {
			this.minTimeToProcess = minTimeToProcess;
			return this;
		}

		public SqsEndpointBuilder<T> queuesAttributes(Map<String, QueueAttributes> queueAttributesMap) {
			this.queuesAttributes = queueAttributesMap;
			return this;
		}

		public SqsEndpointBuilder<T> async(boolean async) {
			this.async = async;
			return this;
		}

		public SqsEndpointBuilder<T> id(String id) {
			this.id = id;
			return this;
		}

		public SqsEndpoint build() {
			return new SqsEndpoint(this.logicalEndpointNames, this.factoryName, this.simultaneousPollsPerQueue,
					this.pollTimeoutSeconds, this.minTimeToProcess, this.queuesAttributes, this.async, this.id);
		}
	}

}
