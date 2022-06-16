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


import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.awspring.cloud.sqs.support.QueueAttributesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.MimeType;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessagePoller<T> extends AbstractMessagePoller<T> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessagePoller.class);

//	private final QueueAttributes queueAttributes;

	private final SqsAsyncClient sqsAsyncClient;

	private String queueUrl;

	public SqsMessagePoller(String logicalEndpointName, SqsAsyncClient sqsClient) {
		super(logicalEndpointName);
		this.sqsAsyncClient = sqsClient;
	}

	@Override
	protected void doStart() {
		if (this.queueUrl == null) {
			this.queueUrl = QueueAttributesProvider.fetchAttributes(super.getLogicalEndpointName(), this.sqsAsyncClient)
				.getDestinationUrl();
		}
	}

	@Override
	protected void doStop() {

	}

	// TODO: Consider a way of producing a Message<POJO> for SQS, so that the MessageListener gets the converted
	//  message. We can do that by inferring the type from the target method - perhaps by passing all method argument resolvers
	//  to exclude useful ones.
	//  We can also have an PayloadConvertingMessageListenerAdapter that can be added, with a method
	//  that receives the target type and the MessageListener.

	@Override
	protected CompletableFuture<Collection<Message<T>>> doPollForMessages(int numberOfMessages, Duration timeout) {
		return sqsAsyncClient
			.receiveMessage(req -> req.queueUrl(this.queueUrl).maxNumberOfMessages(numberOfMessages)
				.waitTimeSeconds((int) timeout.getSeconds()))
			.thenApply(ReceiveMessageResponse::messages)
			.thenApply(this::convertMessages);
	}

	private Collection<Message<T>> convertMessages(List<software.amazon.awssdk.services.sqs.model.Message> messages) {
		return messages.stream().map(this::convertMessage).collect(Collectors.toList());
	}

	protected Message<T> convertMessage(final software.amazon.awssdk.services.sqs.model.Message message) {
		logger.trace("Converting message {}", message);
		HashMap<String, Object> additionalHeaders = new HashMap<>();
		additionalHeaders.put(MessageHeaders.MESSAGE_ID_HEADER, message.messageId());
		additionalHeaders.put(SqsMessageHeaders.SQS_LOGICAL_RESOURCE_ID, getLogicalEndpointName());
		additionalHeaders.put(SqsMessageHeaders.RECEIVED_AT, Instant.now());
		// additionalHeaders.put(SqsMessageHeaders.QUEUE_VISIBILITY, this.queueAttributes.getVisibilityTimeout());
		additionalHeaders.put(SqsMessageHeaders.VISIBILITY,
				new QueueMessageVisibility(this.sqsAsyncClient, this.queueUrl, message.receiptHandle()));
		return createMessage(message, Collections.unmodifiableMap(additionalHeaders));
	}

	protected Message<T> createMessage(
			software.amazon.awssdk.services.sqs.model.Message message, Map<String, Object> additionalHeaders) {

		HashMap<String, Object> messageHeaders = new HashMap<>();
		messageHeaders.put(SqsMessageHeaders.MESSAGE_ID_MESSAGE_ATTRIBUTE_NAME, message.messageId());
		messageHeaders.put(SqsMessageHeaders.RECEIPT_HANDLE_MESSAGE_ATTRIBUTE_NAME, message.receiptHandle());
		messageHeaders.put(SqsMessageHeaders.SOURCE_DATA_HEADER, message);
		messageHeaders.put(MessageHeaders.ACKNOWLEDGMENT_HEADER,
				new SqsAcknowledge(this.sqsAsyncClient, this.queueUrl, message.receiptHandle()));
		messageHeaders.putAll(additionalHeaders);
		messageHeaders.putAll(getAttributesAsMessageHeaders(message));
		messageHeaders.putAll(getMessageAttributesAsMessageHeaders(message));
		return new GenericMessage<>((T) message.body(), new SqsMessageHeaders(messageHeaders));
	}

	private static Map<String, Object> getMessageAttributesAsMessageHeaders(
			software.amazon.awssdk.services.sqs.model.Message message) {

		Map<String, Object> messageHeaders = new HashMap<>();
		for (Map.Entry<MessageSystemAttributeName, String> messageAttribute : message.attributes().entrySet()) {
			if (org.springframework.messaging.MessageHeaders.CONTENT_TYPE.equals(messageAttribute.getKey().name())) {
				messageHeaders.put(org.springframework.messaging.MessageHeaders.CONTENT_TYPE,
						MimeType.valueOf(messageAttribute.getValue()));
			}
			else if (org.springframework.messaging.MessageHeaders.ID.equals(messageAttribute.getKey().name())) {
				messageHeaders.put(org.springframework.messaging.MessageHeaders.ID,
						UUID.fromString(messageAttribute.getValue()));
			}
			else {
				messageHeaders.put(messageAttribute.getKey().name(), messageAttribute.getValue());
			}
		}
		return Collections.unmodifiableMap(messageHeaders);
	}

	private static Map<String, Object> getAttributesAsMessageHeaders(
			software.amazon.awssdk.services.sqs.model.Message message) {
		Map<String, Object> messageHeaders = new HashMap<>();
		for (Map.Entry<MessageSystemAttributeName, String> attributeKeyValuePair : message.attributes().entrySet()) {
			messageHeaders.put(attributeKeyValuePair.getKey().name(), attributeKeyValuePair.getValue());
		}
		return messageHeaders;
	}
}
