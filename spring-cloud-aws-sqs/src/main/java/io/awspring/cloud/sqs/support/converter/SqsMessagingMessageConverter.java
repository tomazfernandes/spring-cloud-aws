package io.awspring.cloud.sqs.support.converter;

import io.awspring.cloud.sqs.support.converter.context.ContextAwareHeaderMapper;
import io.awspring.cloud.sqs.support.converter.context.MessageConversionContext;
import io.awspring.cloud.sqs.support.converter.context.SqsMessageConversionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.HeaderMapper;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.util.Assert;

import java.util.function.Function;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessagingMessageConverter implements PayloadHeaderMessagingMessageConverter<software.amazon.awssdk.services.sqs.model.Message> {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessagingMessageConverter.class);

	private static final MessageConverter DEFAULT_MESSAGE_CONVERTER = new MappingJackson2MessageConverter();

	private MessageConverter payloadMessageConverter = DEFAULT_MESSAGE_CONVERTER;

	private Function<Message<?>, Class<?>> payloadTypeMapper;

	private HeaderMapper<software.amazon.awssdk.services.sqs.model.Message> headerMapper = new SqsHeaderMapper();

	@Override
	public void setPayloadTypeMapper(Function<Message<?>, Class<?>> payloadTypeMapper) {
		Assert.notNull(payloadTypeMapper, "payloadTypeMapper cannot be null");
		this.payloadTypeMapper = payloadTypeMapper;
	}

	@Override
	public void setPayloadMessageConverter(MessageConverter messageConverter) {
		Assert.notNull(messageConverter, "messageConverter cannot be null");
		this.payloadMessageConverter = messageConverter;
	}

	@Override
	public void setHeaderMapper(HeaderMapper<software.amazon.awssdk.services.sqs.model.Message> headerMapper) {
		Assert.notNull(headerMapper, "headerMapper cannot be null");
		this.headerMapper = headerMapper;
	}

	@Override
	public Message<?> toMessagingMessage(software.amazon.awssdk.services.sqs.model.Message message, @Nullable MessageConversionContext context) {
		logger.trace("Converting message {} to messaging message", message.messageId());
		MessageHeaders messageHeaders = createMessageHeaders(message, context);
		return MessageBuilder.createMessage(convertPayload(message, messageHeaders), messageHeaders);
	}

	private MessageHeaders createMessageHeaders(software.amazon.awssdk.services.sqs.model.Message message, MessageConversionContext context) {
		MessageHeaders messageHeaders = this.headerMapper.toHeaders(message);
		return context != null && this.headerMapper instanceof ContextAwareHeaderMapper
			? addContextHeaders(message, context, messageHeaders)
			: messageHeaders;
	}

	private MessageHeaders addContextHeaders(software.amazon.awssdk.services.sqs.model.Message message, MessageConversionContext context, MessageHeaders messageHeaders) {
		MessageHeaders contextHeaders = getContextHeaders(message, context);
		MessageHeaderAccessor accessor = new MessageHeaderAccessor();
		accessor.copyHeaders(messageHeaders);
		accessor.copyHeaders(contextHeaders);
		return accessor.getMessageHeaders();
	}

	private MessageHeaders getContextHeaders(software.amazon.awssdk.services.sqs.model.Message message, MessageConversionContext context) {
		return ((ContextAwareHeaderMapper<software.amazon.awssdk.services.sqs.model.Message>) this.headerMapper).getContextHeaders(message, context);
	}

	@Override
	public MessageConversionContext createMessageConversionContext() {
		return new SqsMessageConversionContext();
	}

	private Object convertPayload(software.amazon.awssdk.services.sqs.model.Message message, MessageHeaders messageHeaders) {
		Message<String> messagingMessage = MessageBuilder.createMessage(message.body(), messageHeaders);
		return this.payloadTypeMapper != null
			? this.payloadMessageConverter.fromMessage(messagingMessage, this.payloadTypeMapper.apply(messagingMessage))
			: message.body();
	}

}
