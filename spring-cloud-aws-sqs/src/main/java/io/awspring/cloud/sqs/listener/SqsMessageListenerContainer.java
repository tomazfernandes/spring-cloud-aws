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

import io.awspring.cloud.sqs.ConfigUtils;
import io.awspring.cloud.sqs.LifecycleUtils;
import io.awspring.cloud.sqs.listener.pipeline.AckHandlerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.ErrorHandlerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.InterceptorExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.MessageListenerExecutionStage;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingConfiguration;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingPipeline;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingPipelineBuilder;
import io.awspring.cloud.sqs.listener.sink.MessageProcessingPipelineSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.sink.TaskExecutorAwareComponent;
import io.awspring.cloud.sqs.listener.source.MessageSource;
import io.awspring.cloud.sqs.listener.source.MessageSourceFactory;
import io.awspring.cloud.sqs.listener.source.PollingMessageSource;
import io.awspring.cloud.sqs.listener.source.SqsMessageSource;

import java.util.Collection;
import java.util.stream.Collectors;

import io.awspring.cloud.sqs.listener.source.SqsMessageSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * {@link MessageListenerContainer} implementation for SQS queues.
 *
 * Components and {@link ContainerOptions} can be changed at runtime and such changes will be valid upon container
 * restart.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessageListenerContainer<T> extends AbstractMessageListenerContainer<T> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainer.class);

	private final MessageSourceFactory<T> DEFAULT_SQS_MESSAGE_SOURCE_FACTORY = new SqsMessageSourceFactory<>();

	private final SqsAsyncClient asyncClient;

	private Collection<MessageSource<T>> messageSources;

	private MessageSink<T> messageSink;

	public SqsMessageListenerContainer(SqsAsyncClient asyncClient, ContainerOptions options) {
		super(options);
		this.asyncClient = asyncClient;
	}

	@Override
	protected void doStart() {
		this.messageSources = createMessageSources();
		this.messageSink = super.getMessageSink();
		configureComponents();
		LifecycleUtils.start(this.messageSink, this.messageSources);
	}

	private Collection<MessageSource<T>> createMessageSources() {
		if (getMessageSourceFactory() == null) {
			setMessageSourceFactory(DEFAULT_SQS_MESSAGE_SOURCE_FACTORY);
		}
		return getQueueNames().stream().map(this::createMessageSource)
			.collect(Collectors.toList());
	}

	private MessageSource<T> createMessageSource(String queueName) {
		MessageSource<T> messageSource = getMessageSourceFactory().create();
		ConfigUtils.INSTANCE
			.acceptIfInstance(messageSource, PollingMessageSource.class, pms -> pms.setPollingEndpointName(queueName));
		return messageSource;
	}

	@SuppressWarnings("unchecked")
	private void configureComponents() {
		ContainerOptions options = getContainerOptions().createCopy();
		options.configure(this.messageSources);
		options.configure(this.messageSink);
		ConfigUtils.INSTANCE
			.acceptManyIfInstance(this.messageSources, SqsMessageSource.class, sms -> sms.setSqsAsyncClient(this.asyncClient))
			.acceptManyIfInstance(this.messageSources, PollingMessageSource.class, sms -> sms.setBackPressureHandler(createBackPressureHandler()))
			.acceptManyIfInstance(this.messageSources, PollingMessageSource.class, sms -> sms.setMessageSink(this.messageSink))
			.acceptIfInstance(this.messageSink, TaskExecutorAwareComponent.class, tea -> tea.setTaskExecutor(getOrCreateSinkTaskExecutor()))
			.acceptIfInstance(this.messageSink, MessageProcessingPipelineSink.class, mls -> mls.setMessagePipeline(getMessageProcessingPipeline()));
	}

	private MessageProcessingPipeline<T> getMessageProcessingPipeline() {
		return MessageProcessingPipelineBuilder
			.<T>first(InterceptorExecutionStage::new)
			.then(MessageListenerExecutionStage::new)
			.thenWrapWith(ErrorHandlerExecutionStage::new)
			.thenWrapWith(AckHandlerExecutionStage::new)
			.build(MessageProcessingConfiguration.<T>builder()
				.interceptors(getMessageInterceptors())
				.messageListener(getMessageListener())
				.errorHandler(getErrorHandler())
				.ackHandler(getAckHandler()).build());
	}

	private SemaphoreBackPressureHandler createBackPressureHandler() {
		return new SemaphoreBackPressureHandler(getContainerOptions().getMaxInFlightMessagesPerQueue(),
			super.getContainerOptions().getSemaphoreAcquireTimeout());
	}

	private TaskExecutor getOrCreateSinkTaskExecutor() {
		return getContainerOptions().getSinkTaskExecutor() != null
			? getContainerOptions().getSinkTaskExecutor()
			: createSinkTaskExecutor();
	}

	private ThreadPoolTaskExecutor createSinkTaskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		int poolSize = getContainerOptions().getMaxInFlightMessagesPerQueue() * this.messageSources.size();
		executor.setMaxPoolSize(poolSize);
		executor.setCorePoolSize(poolSize);
		executor.setThreadNamePrefix(this.getClass().getSimpleName().toLowerCase() + "-");
		executor.afterPropertiesSet();
		return executor;
	}

	@Override
	protected void doStop() {
		LifecycleUtils.stop(this.messageSources, this.messageSink);
		logger.debug("Container {} stopped", getId());
	}

}
