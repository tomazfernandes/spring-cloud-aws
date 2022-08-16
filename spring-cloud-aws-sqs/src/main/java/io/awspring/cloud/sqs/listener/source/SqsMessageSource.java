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

import io.awspring.cloud.sqs.ConfigUtils;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.QueueAttributes;
import io.awspring.cloud.sqs.listener.QueueAttributesAware;
import io.awspring.cloud.sqs.listener.QueueAttributesResolver;
import io.awspring.cloud.sqs.listener.QueueNotFoundStrategy;
import io.awspring.cloud.sqs.listener.SqsAsyncClientAware;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementExecutor;
import io.awspring.cloud.sqs.listener.acknowledgement.ExecutingAcknowledgementProcessor;
import io.awspring.cloud.sqs.listener.acknowledgement.SqsAcknowledgementExecutor;
import io.awspring.cloud.sqs.support.converter.MessagingMessageConverter;
import io.awspring.cloud.sqs.support.converter.SqsMessagingMessageConverter;
import io.awspring.cloud.sqs.support.converter.context.ContextAwareMessagingMessageConverter;
import io.awspring.cloud.sqs.support.converter.context.MessageConversionContext;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
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
public class SqsMessageSource<T> extends AbstractPollingMessageSource<T> implements SqsAsyncClientAware {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageSource.class);

	private static final int MESSAGE_VISIBILITY_DISABLED = -1;

	private SqsAsyncClient sqsAsyncClient;

	private String queueUrl;

	private QueueAttributes queueAttributes;

	private QueueNotFoundStrategy queueNotFoundStrategy;

	private MessagingMessageConverter<Message> messagingMessageConverter;

	private MessageConversionContext messageConversionContext;

	private Collection<QueueAttributeName> queueAttributeNames;

	private Collection<String> messageAttributeNames;

	private Collection<String> messageSystemAttributeNames;

	private int messageVisibility;

	private int pollTimeout;

	@Override
	public void setSqsAsyncClient(SqsAsyncClient sqsAsyncClient) {
		Assert.notNull(sqsAsyncClient, "sqsAsyncClient cannot be null.");
		this.sqsAsyncClient = sqsAsyncClient;
	}

	@Override
	public void configure(ContainerOptions containerOptions) {
		super.configure(containerOptions);
		this.pollTimeout = (int) containerOptions.getPollTimeout().getSeconds();
		this.queueAttributeNames = containerOptions.getQueueAttributeNames();
		this.messageAttributeNames = containerOptions.getMessageAttributeNames();
		this.messageSystemAttributeNames = containerOptions.getMessageSystemAttributeNames();
		this.messagingMessageConverter = getOrCreateMessageConverter(containerOptions);
		this.queueNotFoundStrategy = containerOptions.getQueueNotFoundStrategy();
		this.messageVisibility = containerOptions.getMessageVisibility() != null
				? (int) containerOptions.getMessageVisibility().getSeconds()
				: MESSAGE_VISIBILITY_DISABLED;
	}

	@Override
	protected void doStart() {
		Assert.notNull(this.sqsAsyncClient, "sqsAsyncClient not set.");
		Assert.notNull(this.queueAttributeNames, "queueAttributeNames not set.");
		Assert.notNull(this.messagingMessageConverter, "messagingMessageConverter not set.");
		this.queueAttributes = resolveQueueAttributes();
		this.queueUrl = this.queueAttributes.getQueueUrl();
		this.messageConversionContext = maybeCreateConversionContext();
		configureConversionContextAndAcknowledgement();
	}

	@SuppressWarnings("unchecked")
	private void configureConversionContextAndAcknowledgement() {
		ConfigUtils.INSTANCE
				.acceptIfInstance(this.messageConversionContext, SqsAsyncClientAware.class,
						saca -> saca.setSqsAsyncClient(this.sqsAsyncClient))
				.acceptIfInstance(this.messageConversionContext, QueueAttributesAware.class,
						qaa -> qaa.setQueueAttributes(this.queueAttributes))
				.acceptIfInstance(getAcknowledgmentProcessor(), ExecutingAcknowledgementProcessor.class, eap -> eap
						.setAcknowledgementExecutor(createAndConfigureAcknowledgementExecutor(this.queueAttributes)));
	}

	// @formatter:off
	private QueueAttributes resolveQueueAttributes() {
		return QueueAttributesResolver.builder()
			.queueName(getPollingEndpointName())
			.sqsAsyncClient(this.sqsAsyncClient)
			.queueAttributeNames(this.queueAttributeNames)
			.queueNotFoundStrategy(this.queueNotFoundStrategy).build()
			.resolveQueueAttributes()
			.join();
	}
	// @formatter:on

	protected AcknowledgementExecutor<T> createAndConfigureAcknowledgementExecutor(QueueAttributes queueAttributes) {
		SqsAcknowledgementExecutor<T> executor = createAcknowledgementExecutorInstance();
		executor.setQueueAttributes(queueAttributes);
		executor.setSqsAsyncClient(this.sqsAsyncClient);
		return executor;
	}

	protected SqsAcknowledgementExecutor<T> createAcknowledgementExecutorInstance() {
		return new SqsAcknowledgementExecutor<>();
	}

	@Nullable
	private MessageConversionContext maybeCreateConversionContext() {
		return this.messagingMessageConverter instanceof ContextAwareMessagingMessageConverter
				? ((ContextAwareMessagingMessageConverter<?>) this.messagingMessageConverter)
						.createMessageConversionContext()
				: null;
	}

	// @formatter:off
	@Override
	protected CompletableFuture<Collection<org.springframework.messaging.Message<T>>> doPollForMessages(
			int maxNumberOfMessages) {
		logger.debug("Polling queue {} for {} messages.", this.queueUrl, maxNumberOfMessages);
		return sqsAsyncClient
			.receiveMessage(createRequest(maxNumberOfMessages))
			.thenApply(ReceiveMessageResponse::messages)
			.whenComplete(this::logMessagesReceived)
			.thenApply(this::convertMessages);
	}

	private ReceiveMessageRequest createRequest(int maxNumberOfMessages) {
		ReceiveMessageRequest.Builder builder = ReceiveMessageRequest
			.builder()
			.queueUrl(this.queueUrl)
			.receiveRequestAttemptId(UUID.randomUUID().toString())
			.maxNumberOfMessages(maxNumberOfMessages)
			.attributeNamesWithStrings(this.messageSystemAttributeNames)
			.messageAttributeNames(this.messageAttributeNames)
			.waitTimeSeconds(this.pollTimeout);

		if (this.messageVisibility >= 0) {
			builder.visibilityTimeout(this.messageVisibility);
		}
		return builder.build();
	}

	private Collection<org.springframework.messaging.Message<T>> convertMessages(List<Message> msgs) {
		return msgs.stream()
			.map(this::convertMessage)
			.collect(Collectors.toList());
	}
	// @formatter:on

	@SuppressWarnings("unchecked")
	private org.springframework.messaging.Message<T> convertMessage(Message msg) {
		return this.messagingMessageConverter instanceof ContextAwareMessagingMessageConverter
				? (org.springframework.messaging.Message<T>) getContextAwareConverter().toMessagingMessage(msg,
						this.messageConversionContext)
				: (org.springframework.messaging.Message<T>) this.messagingMessageConverter.toMessagingMessage(msg);
	}

	private ContextAwareMessagingMessageConverter<Message> getContextAwareConverter() {
		return (ContextAwareMessagingMessageConverter<Message>) this.messagingMessageConverter;
	}

	@SuppressWarnings("unchecked")
	private MessagingMessageConverter<Message> getOrCreateMessageConverter(ContainerOptions containerOptions) {
		return containerOptions.getMessageConverter() != null
				? (MessagingMessageConverter<Message>) containerOptions.getMessageConverter()
				: new SqsMessagingMessageConverter();
	}

	private void logMessagesReceived(Collection<Message> v, Throwable t) {
		if (v != null) {
			if (logger.isTraceEnabled()) {
				logger.trace("Received messages {} from queue {}",
						v.stream().map(Message::messageId).collect(Collectors.toList()), this.queueUrl);
			}
			else {
				logger.debug("Received {} messages from queue {}", v.size(), this.queueUrl);
			}
		}
	}

}
