package io.awspring.cloud.sqs.support.converter;

import io.awspring.cloud.sqs.support.converter.context.ContextAwareMessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.HeaderMapper;

import java.util.function.Function;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface PayloadHeaderMessagingMessageConverter<S> extends ContextAwareMessagingMessageConverter<S> {

	void setPayloadTypeMapper(Function<Message<?>, Class<?>> payloadTypeMapper);

	void setPayloadMessageConverter(MessageConverter messageConverter);

	void setHeaderMapper(HeaderMapper<S> headerMapper);

}
