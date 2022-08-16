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

import io.awspring.cloud.sqs.SqsException;
import io.awspring.cloud.sqs.listener.QueueAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class QueueAttributesResolver {

	private static final Logger logger = LoggerFactory.getLogger(QueueAttributes.class);

	private QueueAttributesResolver() {
	}

	public static QueueAttributes resolve(String queueName, SqsAsyncClient sqsAsyncClient, Collection<QueueAttributeName> queueAttributeNames) {
		try {
			logger.debug("Resolving attributes for queue {}", queueName);
			String queueUrl = resolveQueueUrl(queueName, sqsAsyncClient);
			return new QueueAttributes(queueName, queueUrl, getQueueAttributes(sqsAsyncClient, queueAttributeNames, queueUrl, queueName));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new SqsException("Interrupted while resolving attributes for queue " + queueName, e);
		}
		catch (Exception e) {
			throw new SqsException("Error resolving attributes for queue " + queueName, e);
		}
	}

	private static String resolveQueueUrl(String queueName, SqsAsyncClient sqsAsyncClient) throws InterruptedException, ExecutionException {
		return isValidQueueUrl(queueName)
			? queueName
			: sqsAsyncClient.getQueueUrl(req -> req.queueName(queueName)).get().queueUrl();
	}

	private static Map<QueueAttributeName, String> getQueueAttributes(SqsAsyncClient sqsAsyncClient, Collection<QueueAttributeName> queueAttributeNames, String queueUrl, String queueName) throws InterruptedException, ExecutionException {
		return queueAttributeNames.isEmpty()
			? Collections.emptyMap()
			: doGetAttributes(sqsAsyncClient, queueAttributeNames, queueUrl, queueName);
	}

	private static Map<QueueAttributeName, String> doGetAttributes(SqsAsyncClient sqsAsyncClient, Collection<QueueAttributeName> queueAttributeNames, String queueUrl, String queueName) throws InterruptedException, ExecutionException {
		logger.debug("Resolving attributes {} for queue {}", queueAttributeNames, queueName);
		Map<QueueAttributeName, String> attributes = sqsAsyncClient.getQueueAttributes(req -> req.queueUrl(queueUrl).attributeNames(queueAttributeNames)).get().attributes();
		logger.debug("Attributes for queue {} resolved", queueName);
		return attributes;
	}

	private static boolean isValidQueueUrl(String name) {
		try {
			URI candidate = new URI(name);
			return ("http".equals(candidate.getScheme()) || "https".equals(candidate.getScheme()));
		}
		catch (URISyntaxException e) {
			return false;
		}
	}

}
