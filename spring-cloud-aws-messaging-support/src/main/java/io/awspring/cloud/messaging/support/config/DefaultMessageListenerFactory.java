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
import io.awspring.cloud.messaging.support.listener.adapter.AsyncMessagingMessageListenerAdapter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.util.Assert;

/**
 * Create a suitable {@link AsyncMessageListener} instance for a given {@link Endpoint},
 * including any adapters that may be required.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class DefaultMessageListenerFactory<T> implements MessageListenerFactory<T> {

	private MessageHandlerMethodFactory handlerMethodFactory = new DefaultMessageHandlerMethodFactory();

	@Override
	public AsyncMessageListener<T> createMessageListener(Endpoint endpoint) {
		Assert.isInstanceOf(AbstractEndpoint.class, endpoint,
			() -> "Endpoint must be an instance of AbstractEndpoint to be used with this factory. Provided: " + endpoint.getClass());
		Assert.notNull(this.handlerMethodFactory, "No handlerMethodFactory has been set");
		AbstractEndpoint abstractEndpoint = (AbstractEndpoint) endpoint;
		return new AsyncMessagingMessageListenerAdapter<>(
			this.handlerMethodFactory.createInvocableHandlerMethod(abstractEndpoint.getBean(), abstractEndpoint.getMethod()));
	}

	public void setHandlerMethodFactory(MessageHandlerMethodFactory handlerMethodFactory) {
		Assert.notNull(handlerMethodFactory, "handlerMethodFactory cannot be null");
		this.handlerMethodFactory = handlerMethodFactory;
	}
}
