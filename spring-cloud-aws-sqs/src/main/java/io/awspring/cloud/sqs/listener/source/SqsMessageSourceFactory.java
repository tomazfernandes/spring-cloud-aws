package io.awspring.cloud.sqs.listener.source;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessageSourceFactory<T> implements MessageSourceFactory<T> {

	@Override
	public MessageSource<T> create(String endpointName) {
		return new SqsMessageSource<>(endpointName);
	}

}
