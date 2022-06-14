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
package io.awspring.cloud.sqs.support;

import io.awspring.cloud.sqs.listener.QueueAttributes;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class QueueAttributesProvider {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainer.class);

	public static QueueAttributes fetchAttributes(String queue, SqsAsyncClient sqsAsyncClient) {
		try {
			logger.debug("Fetching queue attributes for queue " + queue);
			String queueUrl = sqsAsyncClient.getQueueUrl(req -> req.queueName(queue)).get().queueUrl();
			Map<QueueAttributeName, String> attributes = sqsAsyncClient
				.getQueueAttributes(req -> req.queueUrl(queueUrl)).get().attributes();
			boolean hasRedrivePolicy = attributes.containsKey(QueueAttributeName.REDRIVE_POLICY);
			boolean isFifo = queue.endsWith(".fifo");
			return new QueueAttributes(queueUrl, hasRedrivePolicy, getVisibility(attributes), isFifo);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while fetching attributes for queue " + queue);
		}
		catch (ExecutionException e) {
			throw new IllegalStateException("ExecutionException while fetching attributes for queue " + queue);
		}
	}

	private static Integer getVisibility(Map<QueueAttributeName, String> attributes) {
		String visibilityTimeout = attributes.get(QueueAttributeName.VISIBILITY_TIMEOUT);
		return visibilityTimeout != null ? Integer.parseInt(visibilityTimeout) : null;
	}

}
