package io.awspring.cloud.sqs.listener.source;

import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.support.converter.MessagingMessageConverter;
import io.awspring.cloud.sqs.support.converter.context.ContextAwareMessagingMessageConverter;
import io.awspring.cloud.sqs.support.converter.context.MessageConversionContext;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

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
		return messages.stream()
			.map(this::convert)
			.collect(Collectors.toList());
	}
	// @formatter:on

	@SuppressWarnings("unchecked")
	private Message<T> convert(S msg) {
		return this.messagingMessageConverter instanceof ContextAwareMessagingMessageConverter
			? (Message<T>) getContextAwareConverter().toMessagingMessage(msg,
			this.messageConversionContext)
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
