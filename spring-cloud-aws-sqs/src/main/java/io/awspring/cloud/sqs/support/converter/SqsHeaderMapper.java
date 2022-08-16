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
package io.awspring.cloud.sqs.support.converter;

import io.awspring.cloud.sqs.ConfigUtils;
import io.awspring.cloud.sqs.listener.QueueAttributes;
import io.awspring.cloud.sqs.listener.QueueMessageVisibility;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.acknowledgement.SqsAcknowledgement;
import io.awspring.cloud.sqs.support.converter.context.ContextAwareHeaderMapper;
import io.awspring.cloud.sqs.support.converter.context.MessageConversionContext;
import io.awspring.cloud.sqs.support.converter.context.SqsMessageConversionContext;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageHeaderAccessor;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsHeaderMapper implements ContextAwareHeaderMapper<Message> {

	private static final Logger logger = LoggerFactory.getLogger(SqsHeaderMapper.class);

	@Override
	public void fromHeaders(MessageHeaders headers, Message target) {
		// We'll probably use this for SqsTemplate later
	}

	@Override
	public MessageHeaders toHeaders(Message source) {
		logger.trace("Mapping headers for message {}", source.messageId());
		MessageHeaderAccessor accessor = new MessageHeaderAccessor();
		accessor.copyHeadersIfAbsent(getMessageSystemAttributesAsHeaders(source));
		accessor.copyHeadersIfAbsent(getMessageAttributesAsHeaders(source));
		accessor.setHeader(SqsHeaders.SQS_MESSAGE_ID_HEADER, UUID.fromString(source.messageId()));
		accessor.setHeader(SqsHeaders.SQS_RECEIPT_HANDLE_HEADER, source.receiptHandle());
		accessor.setHeader(SqsHeaders.SQS_SOURCE_DATA_HEADER, source);
		accessor.setHeader(SqsHeaders.SQS_RECEIVED_AT_HEADER, Instant.now());
		MessageHeaders messageHeaders = accessor.toMessageHeaders();
		logger.trace("Mapped headers {} for message {}", messageHeaders, source.messageId());
		return messageHeaders;
	}

	private Map<String, String> getMessageAttributesAsHeaders(Message source) {
		return source
			.messageAttributes()
			.entrySet()
			.stream()
			.collect(Collectors.toMap(entry -> SqsHeaders.SQS_MA_HEADER_PREFIX + entry.getKey(), entry -> entry.getValue().stringValue()));
	}

	private Map<String, String> getMessageSystemAttributesAsHeaders(Message source) {
		return source
			.attributes()
			.entrySet()
			.stream()
			.collect(Collectors.toMap(entry -> SqsHeaders.MessageSystemAttribute.SQS_MSA_HEADER_PREFIX + entry.getKey(), Map.Entry::getValue));
	}

	@Override
	public MessageHeaders createContextHeaders(Message source, MessageConversionContext context) {
		logger.trace("Creating context headers for message {}", source.messageId());
		MessageHeaderAccessor accessor = new MessageHeaderAccessor();
		ConfigUtils.INSTANCE.acceptIfInstance(context, SqsMessageConversionContext.class,
				sqsContext -> addSqsContextHeaders(source, sqsContext, accessor));
		MessageHeaders messageHeaders = accessor.toMessageHeaders();
		logger.trace("Context headers {} created for message {}", messageHeaders, source.messageId());
		return messageHeaders;
	}

	private void addSqsContextHeaders(Message source, SqsMessageConversionContext sqsContext,
			MessageHeaderAccessor accessor) {
		QueueAttributes queueAttributes = sqsContext.getQueueAttributes();
		SqsAsyncClient sqsAsyncClient = sqsContext.getSqsAsyncClient();
		accessor.setHeader(SqsHeaders.SQS_QUEUE_NAME_HEADER, queueAttributes.getQueueName());
		accessor.setHeader(SqsHeaders.SQS_QUEUE_URL_HEADER, queueAttributes.getQueueUrl());
		accessor.setHeader(SqsHeaders.SQS_QUEUE_ATTRIBUTES_HEADER, queueAttributes);
		accessor.setHeader(SqsHeaders.SQS_ACKNOWLEDGMENT_HEADER, new SqsAcknowledgement(sqsAsyncClient,
				queueAttributes.getQueueUrl(), source.receiptHandle(), source.messageId()));
		accessor.setHeader(SqsHeaders.SQS_VISIBILITY_HEADER,
				new QueueMessageVisibility(sqsAsyncClient, queueAttributes.getQueueUrl(), source.receiptHandle()));
	}
}
