package io.awspring.cloud.sqs.listener;

import io.awspring.cloud.sqs.listener.acknowledgement.AckHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.OnSuccessAckHandler;
import io.awspring.cloud.sqs.listener.sink.BatchMessageSink;
import io.awspring.cloud.sqs.listener.sink.FanOutMessageSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.source.MessageSource;
import io.awspring.cloud.sqs.listener.source.SqsMessageSource;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class StandardSqsComponentFactory<T> implements ContainerComponentFactory<T> {

	private MessageDeliveryStrategy messageDeliveryStrategy;

	public void configure(ContainerOptions containerOptions) {
		this.messageDeliveryStrategy = containerOptions.getMessageDeliveryStrategy();
	}

	@Override
	public MessageSource<T> createMessageSource() {
		return new SqsMessageSource<>();
	}

	@Override
	public MessageSink<T> createMessageSink() {
		return MessageDeliveryStrategy.SINGLE_MESSAGE.equals(this.messageDeliveryStrategy)
			? new FanOutMessageSink<>()
			: new BatchMessageSink<>();
	}

	@Override
	public AckHandler<T> createAckHandler() {
		return new OnSuccessAckHandler<>();
	}

}
