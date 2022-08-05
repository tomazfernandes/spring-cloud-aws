package io.awspring.cloud.sqs.listener;

import io.awspring.cloud.sqs.ConfigUtils;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementOrdering;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementProcessor;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AlwaysAcknowledgementHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.NeverAcknowledgementHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.OnSuccessAcknowledgementHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.SqsAcknowledgementProcessor;
import io.awspring.cloud.sqs.listener.sink.BatchMessageSink;
import io.awspring.cloud.sqs.listener.sink.FanOutMessageSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.source.MessageSource;
import io.awspring.cloud.sqs.listener.source.SqsMessageSource;

import java.time.Duration;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class StandardSqsComponentFactory<T> implements ContainerComponentFactory<T> {

	private static final Duration DEFAULT_STANDARD_SQS_ACK_INTERVAL = Duration.ofSeconds(1);

	private static final Integer DEFAULT_STANDARD_SQS_ACK_THRESHOLD = 10;

	private static final AcknowledgementOrdering DEFAULT_STANDARD_SQS_ACK_ORDERING = AcknowledgementOrdering.PARALLEL;

	@Override
	public MessageSource<T> createMessageSource(ContainerOptions options) {
		return new SqsMessageSource<>();
	}

	@Override
	public MessageSink<T> createMessageSink(ContainerOptions options) {
		return MessageDeliveryStrategy.SINGLE_MESSAGE.equals(options.getMessageDeliveryStrategy())
			? new FanOutMessageSink<>()
			: new BatchMessageSink<>();
	}

	@Override
	public AcknowledgementHandler<T> createAcknowledgementHandler(ContainerOptions options) {
		AcknowledgementMode mode = options.getAcknowledgementMode();
		return AcknowledgementMode.ON_SUCCESS.equals(mode)
			? new OnSuccessAcknowledgementHandler<>()
			: AcknowledgementMode.ALWAYS.equals(mode)
				? new AlwaysAcknowledgementHandler<>()
				: new NeverAcknowledgementHandler<>();
	}

	@Override
	public AcknowledgementProcessor<T> createAcknowledgementProcessor(ContainerOptions options) {
		SqsAcknowledgementProcessor<T> processor = new SqsAcknowledgementProcessor<>();
		ConfigUtils.INSTANCE
			.acceptIfNotNullOrElse(processor::setAcknowledgementInterval, options.getAcknowledgementInterval(), DEFAULT_STANDARD_SQS_ACK_INTERVAL)
			.acceptIfNotNullOrElse(processor::setAcknowledgementThreshold, options.getAcknowledgementThreshold(), DEFAULT_STANDARD_SQS_ACK_THRESHOLD)
			.acceptIfNotNullOrElse(processor::setAcknowledgementOrdering, options.getAcknowledgementOrdering(), DEFAULT_STANDARD_SQS_ACK_ORDERING);
		return processor;
	}

}
