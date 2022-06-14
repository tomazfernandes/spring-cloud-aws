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
package io.awspring.cloud.messaging.support.config;

import io.awspring.cloud.messaging.support.listener.AsyncMessageListener;
import io.awspring.cloud.messaging.support.listener.MessageListenerContainer;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.lang.reflect.Method;
import java.util.Collection;

/**
 * Base class for implementing an {@link Endpoint}.
 *
 * Contains properties that should be common to all endpoints that will be handled
 * by an {@link AbstractMessageListenerContainerFactory}.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractEndpoint<T> implements Endpoint<T> {

	private final Collection<String> logicalNames;

	private final String listenerContainerFactoryName;

	private final String id;

	private Object bean;

	private Method method;

	private MessageListenerFactory<T> messageListenerFactory;

	protected AbstractEndpoint(Collection<String> logicalNames,
							   @Nullable String listenerContainerFactoryName,
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

	public void setBean(Object bean) {
		this.bean = bean;
	}

	public void setMethod(Method method) {
		this.method = method;
	}

	/**
	 * Set the {@link MessageListenerFactory} that will create the
	 * {@link AsyncMessageListener} to be used with this {@link Endpoint}.
	 * @param messageListenerFactory the factory instance.
	 */
	public void setMessageListenerFactory(MessageListenerFactory<T> messageListenerFactory) {
		this.messageListenerFactory = messageListenerFactory;
	}

	public void setupMessageListener(MessageListenerContainer container) {
		container.setMessageListener(this.messageListenerFactory.createFor(this));
	}

	public Method getMethod() {
		return this.method;
	}

	public Object getBean() {
		return this.bean;
	}
}
