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
package io.awspring.cloud.sqs.listener.source;

import io.awspring.cloud.sqs.listener.QueueAttributes;
import io.awspring.cloud.sqs.listener.QueueAttributesResolver;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import io.awspring.cloud.sqs.support.SqsMessageConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * {@link MessageSource} implementation for polling messages from a SQS queue and converting them to messaging
 * {@link Message}.
 *
 * <p>
 * A {@link io.awspring.cloud.sqs.listener.MessageListenerContainer} can contain many sources, and each source polls
 * from a single queue.
 * </p>
 *
 * <p>
 * Note that currently the payload is not converted here and is returned as String. The actual conversion to the
 * {@link io.awspring.cloud.sqs.annotation.SqsListener} argument type happens on
 * {@link org.springframework.messaging.handler.invocation.InvocableHandlerMethod} invocation.
 * </p>
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessageSource<T> extends AbstractPollingMessageSource<T> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageSource.class);

	private SqsAsyncClient sqsAsyncClient;

	private String queueUrl;

	private SqsMessageConverter<T> sqsMessageConverter;

	public void setSqsAsyncClient(SqsAsyncClient sqsAsyncClient) {
		Assert.notNull(sqsAsyncClient, "sqsAsyncClient cannot be null.");
		this.sqsAsyncClient = sqsAsyncClient;
	}

	@Override
	protected void doStart() {
		Assert.state(this.sqsAsyncClient != null, "sqsAsyncClient not set");
		QueueAttributes queueAttributes = QueueAttributesResolver.resolveAttributes(getPollingEndpointName(),
				this.sqsAsyncClient);
		this.queueUrl = queueAttributes.getQueueUrl();
		this.sqsMessageConverter = new SqsMessageConverter<>(getPollingEndpointName(), this.sqsAsyncClient, queueAttributes);
		super.doStart();
	}

	@Override
	protected CompletableFuture<Collection<Message<T>>> doPollForMessages(int messagesToRequest) {
		logger.trace("Polling queue {} for {} messages.", this.queueUrl, getMessagesPerPoll());
		return sqsAsyncClient
				.receiveMessage(req -> req.queueUrl(this.queueUrl).maxNumberOfMessages(messagesToRequest)
					.attributeNames(QueueAttributeName.ALL).waitTimeSeconds((int) getPollTimeout().getSeconds()))
				.thenApply(ReceiveMessageResponse::messages).thenApply(this.sqsMessageConverter::toMessagingMessages);
	}

}
