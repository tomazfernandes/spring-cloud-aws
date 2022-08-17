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

import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.support.converter.MessagingMessageConverter;
import io.awspring.cloud.sqs.support.converter.context.ContextAwareMessagingMessageConverter;
import io.awspring.cloud.sqs.support.converter.context.MessageConversionContext;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractMessageConvertingMessageSource<T, S> implements MessageSource<T> {

	private MessagingMessageConverter<S> messagingMessageConverter;

	private MessageConversionContext messageConversionContext;

	@Override
	public void configure(ContainerOptions containerOptions) {
		this.messagingMessageConverter = getOrCreateMessageConverter(containerOptions);
		this.messageConversionContext = maybeCreateConversionContext();
		Assert.notNull(this.messagingMessageConverter, "messagingMessageConverter not set.");
	}

	@Nullable
	private MessageConversionContext maybeCreateConversionContext() {
		return this.messagingMessageConverter instanceof ContextAwareMessagingMessageConverter
				? ((ContextAwareMessagingMessageConverter<?>) this.messagingMessageConverter)
						.createMessageConversionContext()
				: null;
	}

	protected Collection<Message<T>> convert(List<S> messages) {
		return messages.stream().map(this::convert).collect(Collectors.toList());
	}

	@SuppressWarnings("unchecked")
	private Message<T> convert(S msg) {
		return this.messagingMessageConverter instanceof ContextAwareMessagingMessageConverter
				? (Message<T>) getContextAwareConverter().toMessagingMessage(msg, this.messageConversionContext)
				: (Message<T>) this.messagingMessageConverter.toMessagingMessage(msg);
	}

	private ContextAwareMessagingMessageConverter<S> getContextAwareConverter() {
		return (ContextAwareMessagingMessageConverter<S>) this.messagingMessageConverter;
	}

	@SuppressWarnings("unchecked")
	private MessagingMessageConverter<S> getOrCreateMessageConverter(ContainerOptions containerOptions) {
		return containerOptions.getMessageConverter() != null
				? (MessagingMessageConverter<S>) containerOptions.getMessageConverter()
				: createMessageConverter();
	}

	protected abstract MessagingMessageConverter<S> createMessageConverter();

	protected MessageConversionContext getMessageConversionContext() {
		return this.messageConversionContext;
	}

}
