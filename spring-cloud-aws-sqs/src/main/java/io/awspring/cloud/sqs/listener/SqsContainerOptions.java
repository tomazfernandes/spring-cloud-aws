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

import io.awspring.cloud.messaging.support.listener.AbstractContainerOptions;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.Map;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsContainerOptions extends AbstractContainerOptions<SqsContainerOptions> {

	private final Map<String, QueueAttributes> queuesAttributes;

	private Integer minTimeToProcess;

	private SqsContainerOptions(Map<String, QueueAttributes> queuesAttributes) {
		this.queuesAttributes = queuesAttributes;
	}

	public static SqsContainerOptions create(Map<String, QueueAttributes> queuesAttributes) {
		return new SqsContainerOptions(queuesAttributes);
	}

	public SqsContainerOptions minTimeToProcess(Integer minTimeToProcess) {
		this.minTimeToProcess = minTimeToProcess;
		return this;
	}

	public Integer getMinTimeToProcess() {
		return minTimeToProcess;
	}

	public Map<String, QueueAttributes> getQueuesAttributes() {
		return queuesAttributes;
	}
}
