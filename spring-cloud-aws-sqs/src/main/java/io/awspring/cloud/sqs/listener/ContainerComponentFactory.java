package io.awspring.cloud.sqs.listener;

import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementProcessor;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AlwaysAcknowledgementHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.NeverAcknowledgementHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.OnSuccessAcknowledgementHandler;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.source.MessageSource;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface ContainerComponentFactory<T> {

	MessageSource<T> createMessageSource(ContainerOptions options);

	MessageSink<T> createMessageSink(ContainerOptions options);

	AcknowledgementProcessor<T> createAcknowledgementProcessor(ContainerOptions options);

	default AcknowledgementHandler<T> createAcknowledgementHandler(ContainerOptions options) {
		AcknowledgementMode mode = options.getAcknowledgementMode();
		return AcknowledgementMode.ON_SUCCESS.equals(mode)
			? new OnSuccessAcknowledgementHandler<>()
			: AcknowledgementMode.ALWAYS.equals(mode)
				? new AlwaysAcknowledgementHandler<>()
				: new NeverAcknowledgementHandler<>();
	}

}
