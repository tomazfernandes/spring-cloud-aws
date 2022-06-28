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
package io.awspring.cloud.sqs.config;

import io.awspring.cloud.sqs.listener.AbstractMessageListenerContainer;
import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import io.awspring.cloud.sqs.listener.adapter.AsyncMessagingMessageListenerAdapter;
import io.awspring.cloud.sqs.listener.sink.MessageListeningSink;
import io.awspring.cloud.sqs.listener.sink.FanOutMessageSink;
import java.lang.reflect.Method;
import java.util.Collection;
import org.springframework.lang.Nullable;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.util.Assert;

/**
 * Base class for implementing an {@link Endpoint}. Contains properties that should be common to all endpoints to be
 * handled by an {@link AbstractMessageListenerContainerFactory}.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractEndpoint implements Endpoint {

	private final Collection<String> logicalNames;

	private final String listenerContainerFactoryName;

	private final String id;

	private Object bean;

	private Method method;

	private MessageHandlerMethodFactory handlerMethodFactory;

	private MessageListeningSink<?> messageSink;

	protected AbstractEndpoint(Collection<String> logicalNames, @Nullable String listenerContainerFactoryName,
			String id) {
		Assert.notEmpty(logicalNames, "logicalNames cannot be empty.");
		this.logicalNames = logicalNames;
		this.listenerContainerFactoryName = listenerContainerFactoryName;
		this.id = id;
	}

	@Override
	public Collection<String> getLogicalNames() {
		return this.logicalNames;
	}

	@Override
	public String getListenerContainerFactoryName() {
		return this.listenerContainerFactoryName;
	}

	@Override
	public String getId() {
		return this.id;
	}

	/**
	 * Set the bean instance to be used when handling a message for this endpoint.
	 * @param bean the bean instance.
	 */
	public void setBean(Object bean) {
		this.bean = bean;
	}

	/**
	 * Set the method to be used when handling a message for this endpoint.
	 * @param method the method.
	 */
	public void setMethod(Method method) {
		this.method = method;
	}

	/**
	 * Set a {@link MessageListeningSink} to handle messages polled from this endpoint. If none is provided, one will be
	 * created depending on the endpoint's configuration.
	 * @param messageSink the sink.
	 */
	public void setMessageSink(MessageListeningSink<?> messageSink) {
		this.messageSink = messageSink;
	}

	/**
	 * Configure the provided container for this endpoint.
	 * @param container the container to be configured.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void setupContainer(MessageListenerContainer container) {
		container.setAsyncMessageListener(createMessageListener());
		if (container instanceof AbstractMessageListenerContainer) {
			((AbstractMessageListenerContainer) container).setMessageSink(createOrGetMessageSplitter());
		}
	}

	private MessageListeningSink<?> createOrGetMessageSplitter() {
		return this.messageSink != null ? this.messageSink : new FanOutMessageSink<>();
	}

	private AsyncMessageListener<?> createMessageListener() {
		Assert.notNull(this.handlerMethodFactory, "No handlerMethodFactory has been set");
		return new AsyncMessagingMessageListenerAdapter<>(
				this.handlerMethodFactory.createInvocableHandlerMethod(this.bean, this.method));
	}

	/**
	 * Set the {@link MessageHandlerMethodFactory} to be used for handling messages in this endpoint.
	 * @param handlerMethodFactory the factory.
	 */
	public void setHandlerMethodFactory(MessageHandlerMethodFactory handlerMethodFactory) {
		Assert.notNull(handlerMethodFactory, "handlerMethodFactory cannot be null");
		this.handlerMethodFactory = handlerMethodFactory;
	}

}
