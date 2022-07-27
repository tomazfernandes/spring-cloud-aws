package io.awspring.cloud.sqs.listener;

import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.acknowledgement.AckHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.OnSuccessAckHandler;
import io.awspring.cloud.sqs.listener.sink.BatchMessageSink;
import io.awspring.cloud.sqs.listener.sink.FanOutMessageSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.sink.OrderedMessageListeningSink;
import io.awspring.cloud.sqs.listener.sink.adapter.MessageGroupingSinkAdapter;
import io.awspring.cloud.sqs.listener.source.MessageSource;
import io.awspring.cloud.sqs.listener.source.SqsMessageSource;
import org.springframework.messaging.Message;

import java.util.Objects;
import java.util.function.Function;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class FifoSqsComponentFactory<T> implements ContainerComponentFactory<T> {

	private MessageDeliveryStrategy messageDeliveryStrategy;

	@Override
	public void configure(ContainerOptions containerOptions) {
		this.messageDeliveryStrategy = containerOptions.getMessageDeliveryStrategy();
	}

	@Override
	public MessageSource<T> createMessageSource() {
		return new SqsMessageSource<>();
	}

	@Override
	public MessageSink<T> createMessageSink() {
		return new MessageGroupingSinkAdapter<>(getDeliverySink(), getMessageHeader());
	}

	private MessageSink<T> getDeliverySink() {
		return MessageDeliveryStrategy.SINGLE_MESSAGE.equals(this.messageDeliveryStrategy)
			? new OrderedMessageListeningSink<>()
			: new BatchMessageSink<>();
	}

	private Function<Message<T>, String> getMessageHeader() {
		return message -> MessageHeaderUtils.getHeader(message, SqsMessageHeaders.SQS_GROUP_ID_HEADER, String.class);
	}

	@Override
	public AckHandler<T> createAckHandler() {
		return new OnSuccessAckHandler<>();
	}

}
