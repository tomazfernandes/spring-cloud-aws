package io.awspring.cloud.sqs.listener.source;

import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementProcessor;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface AcknowledgementProcessingMessageSource<T> extends MessageSource<T> {

	void setAcknowledgementProcessor(AcknowledgementProcessor<T> acknowledgementProcessor);

}
