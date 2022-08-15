package io.awspring.cloud.sqs.listener.acknowledgement;

import io.awspring.cloud.sqs.SqsException;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsAcknowledgementException extends SqsException {

	private final Collection<Message<?>> failedAcknowledgements;

	private final String queueUrl;

	public <T> SqsAcknowledgementException(String errorMessage, Collection<Message<T>> failedAcknowledgements, String queueUrl, Throwable e) {
		super(errorMessage, e);
		this.queueUrl = queueUrl;
		this.failedAcknowledgements = failedAcknowledgements
			.stream()
			.map(msg -> (Message<?>) msg)
			.collect(Collectors.toList());
	}

	public Collection<Message<?>> getFailedAcknowledgements() {
		return this.failedAcknowledgements;
	}

	public String getQueueUrl() {
		return this.queueUrl;
	}

}
