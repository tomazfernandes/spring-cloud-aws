package io.awspring.cloud.sqs.listener;

import io.awspring.cloud.sqs.ConfigUtils;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementOrdering;
import io.awspring.cloud.sqs.listener.acknowledgement.BatchingAcknowledgementProcessor;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementProcessor;
import io.awspring.cloud.sqs.listener.acknowledgement.ImmediateAcknowledgementProcessor;
import io.awspring.cloud.sqs.listener.sink.BatchMessageSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.sink.OrderedMessageSink;
import io.awspring.cloud.sqs.listener.sink.adapter.MessageGroupingSinkAdapter;
import io.awspring.cloud.sqs.listener.sink.adapter.MessageVisibilityExtendingSinkAdapter;
import io.awspring.cloud.sqs.listener.source.MessageSource;
import io.awspring.cloud.sqs.listener.source.SqsMessageSource;
import org.springframework.messaging.Message;

import java.time.Duration;
import java.util.function.Function;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class FifoSqsComponentFactory<T> implements ContainerComponentFactory<T> {

	// Defaults to immediate (sync) ack
	private static final Duration DEFAULT_FIFO_SQS_ACK_INTERVAL = Duration.ZERO;

	private static final Integer DEFAULT_FIFO_SQS_ACK_THRESHOLD = 0;

	// Since immediate acks hold the thread until done, we can execute in parallel and use processing order
	private static final AcknowledgementOrdering DEFAULT_FIFO_SQS_ACK_ORDERING_IMMEDIATE = AcknowledgementOrdering.PARALLEL;

	private static final AcknowledgementOrdering DEFAULT_FIFO_SQS_ACK_ORDERING_BATCHING = AcknowledgementOrdering.ORDERED;

	@Override
	public MessageSource<T> createMessageSource(ContainerOptions options) {
		return new SqsMessageSource<>();
	}

	@Override
	public MessageSink<T> createMessageSink(ContainerOptions options) {
		MessageSink<T> deliverySink = createDeliverySink(options.getMessageDeliveryStrategy());
		return new MessageGroupingSinkAdapter<>(maybeWrapWithVisibilityAdapter(deliverySink, options.getMessageVisibility()), getMessageGroupingHeader());
	}

	private MessageSink<T> createDeliverySink(MessageDeliveryStrategy messageDeliveryStrategy) {
		return MessageDeliveryStrategy.SINGLE_MESSAGE.equals(messageDeliveryStrategy)
			? new OrderedMessageSink<>()
			: new BatchMessageSink<>();
	}

	private MessageSink<T> maybeWrapWithVisibilityAdapter(MessageSink<T> deliverySink, Duration messageVisibility) {
		return messageVisibility != null
			? addMessageVisibilityExtendingSinkAdapter(deliverySink, messageVisibility)
			: deliverySink;
	}

	private MessageVisibilityExtendingSinkAdapter<T> addMessageVisibilityExtendingSinkAdapter(MessageSink<T> deliverySink, Duration messageVisibility) {
		MessageVisibilityExtendingSinkAdapter<T> visibilityAdapter = new MessageVisibilityExtendingSinkAdapter<>(deliverySink);
		visibilityAdapter.setMessageVisibility(messageVisibility);
		return visibilityAdapter;
	}

	private Function<Message<T>, String> getMessageGroupingHeader() {
		return message -> message.getHeaders().get(SqsHeaders.MessageSystemAttribute.SQS_MESSAGE_GROUP_ID_HEADER, String.class);
	}

	@Override
	public AcknowledgementProcessor<T> createAcknowledgementProcessor(ContainerOptions options) {
		return (options.getAcknowledgementInterval() == null || DEFAULT_FIFO_SQS_ACK_INTERVAL.equals(options.getAcknowledgementInterval()))
			&& (options.getAcknowledgementThreshold() == null || DEFAULT_FIFO_SQS_ACK_THRESHOLD.equals(options.getAcknowledgementThreshold()))
				? createAndConfigureImmediateProcessor(options)
				: createAndConfigureBatchingAckProcessor(options);
	}

	protected ImmediateAcknowledgementProcessor<T> createAndConfigureImmediateProcessor(ContainerOptions options) {
		ImmediateAcknowledgementProcessor<T> processor = new ImmediateAcknowledgementProcessor<>();
		processor.setMaxAcknowledgementsPerBatch(10);
		ConfigUtils.INSTANCE
			.acceptIfNotNullOrElse(processor::setAcknowledgementOrdering, options.getAcknowledgementOrdering(), DEFAULT_FIFO_SQS_ACK_ORDERING_IMMEDIATE);
		return processor;
	}

	protected BatchingAcknowledgementProcessor<T> createAndConfigureBatchingAckProcessor(ContainerOptions options) {
		BatchingAcknowledgementProcessor<T> processor = new BatchingAcknowledgementProcessor<>();
		processor.setMaxAcknowledgementsPerBatch(10);
		ConfigUtils.INSTANCE
			.acceptIfNotNullOrElse(processor::setAcknowledgementInterval, options.getAcknowledgementInterval(), DEFAULT_FIFO_SQS_ACK_INTERVAL)
			.acceptIfNotNullOrElse(processor::setAcknowledgementThreshold, options.getAcknowledgementThreshold(), DEFAULT_FIFO_SQS_ACK_THRESHOLD)
			.acceptIfNotNullOrElse(processor::setAcknowledgementOrdering, options.getAcknowledgementOrdering(), DEFAULT_FIFO_SQS_ACK_ORDERING_BATCHING);
		return processor;
	}

}
