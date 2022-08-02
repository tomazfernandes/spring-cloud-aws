package io.awspring.cloud.sqs.listener;

import io.awspring.cloud.sqs.listener.acknowledgement.AckHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.OnSuccessAckHandler;
import io.awspring.cloud.sqs.listener.sink.BatchMessageSink;
import io.awspring.cloud.sqs.listener.sink.FanOutMessageSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.sink.adapter.MessageVisibilityExtendingSinkAdapter;
import io.awspring.cloud.sqs.listener.source.MessageSource;
import io.awspring.cloud.sqs.listener.source.SqsMessageSource;

import java.time.Duration;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class StandardSqsComponentFactory<T> implements ContainerComponentFactory<T> {

	@Override
	public MessageSource<T> createMessageSource(ContainerOptions options) {
		return new SqsMessageSource<>();
	}

	@Override
	public MessageSink<T> createMessageSink(ContainerOptions options) {
		MessageSink<T> deliverySink = createDeliverySink(options.getMessageDeliveryStrategy());
		return maybeWrapWithVisibilityAdapter(deliverySink, options.getMessageVisibility());
	}

	private MessageSink<T> maybeWrapWithVisibilityAdapter(MessageSink<T> deliverySink, Duration messageVisibility) {
		return messageVisibility != null
			? addMessageVisibilityExtendingSinkAdapter(deliverySink, messageVisibility)
			: deliverySink;
	}

	private MessageVisibilityExtendingSinkAdapter<T> addMessageVisibilityExtendingSinkAdapter(MessageSink<T> deliverySink, Duration messageVisibility) {
		MessageVisibilityExtendingSinkAdapter<T> visibilityAdapter = new MessageVisibilityExtendingSinkAdapter<>(deliverySink);
		visibilityAdapter.setVisibilityStrategy(MessageVisibilityExtendingSinkAdapter.Strategy.MESSAGES_BEING_PROCESSED);
		visibilityAdapter.setMessageVisibility(messageVisibility);
		return visibilityAdapter;
	}

	private MessageSink<T> createDeliverySink(MessageDeliveryStrategy messageDeliveryStrategy) {
		return MessageDeliveryStrategy.SINGLE_MESSAGE.equals(messageDeliveryStrategy)
			? new FanOutMessageSink<>()
			: new BatchMessageSink<>();
	}

	@Override
	public AckHandler<T> createAckHandler(ContainerOptions options) {
		return new OnSuccessAckHandler<>();
	}

}
