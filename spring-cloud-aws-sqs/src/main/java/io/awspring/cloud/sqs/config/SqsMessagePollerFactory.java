package io.awspring.cloud.sqs.config;

import io.awspring.cloud.messaging.support.config.AbstractMessagePollerFactory;
import io.awspring.cloud.messaging.support.listener.AsyncMessagePoller;
import io.awspring.cloud.sqs.listener.QueueAttributes;
import io.awspring.cloud.sqs.listener.SqsMessagePoller;
import io.awspring.cloud.sqs.support.QueueAttributesProvider;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsMessagePollerFactory extends AbstractMessagePollerFactory<String> {

	private final SqsAsyncClientSupplier sqsAsyncClientSupplier;

	public SqsMessagePollerFactory(SqsAsyncClientSupplier sqsAsyncClientSupplier) {
		Assert.notNull(sqsAsyncClientSupplier, "sqsAsyncClientSupplier cannot be null.");
		this.sqsAsyncClientSupplier = sqsAsyncClientSupplier;
	}

	@Override
	protected Collection<AsyncMessagePoller<String>> doCreateProducers(Collection<String> endpointNames) {
		SqsAsyncClient sqsAsyncClient = this.sqsAsyncClientSupplier.createNewInstance();
		return endpointNames.stream().map(name -> createSqsProducer(name, sqsAsyncClient)).collect(Collectors.toList());
	}

	private AsyncMessagePoller<String> createSqsProducer(String endpointName, SqsAsyncClient sqsAsyncClient) {
		return new SqsMessagePoller(endpointName, sqsAsyncClient);
	}
}
