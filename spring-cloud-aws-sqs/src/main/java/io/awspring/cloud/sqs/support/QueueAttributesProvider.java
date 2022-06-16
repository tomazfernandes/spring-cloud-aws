package io.awspring.cloud.sqs.support;

import io.awspring.cloud.sqs.listener.QueueAttributes;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class QueueAttributesProvider {

	private static final Logger logger = LoggerFactory.getLogger(SqsMessageListenerContainer.class);

	public static QueueAttributes fetchAttributes(String queue, SqsAsyncClient sqsAsyncClient) {
		try {
			logger.debug("Fetching queue attributes for queue " + queue);
			String queueUrl = sqsAsyncClient.getQueueUrl(req -> req.queueName(queue)).get().queueUrl();
			Map<QueueAttributeName, String> attributes = sqsAsyncClient
				.getQueueAttributes(req -> req.queueUrl(queueUrl)).get().attributes();
			boolean hasRedrivePolicy = attributes.containsKey(QueueAttributeName.REDRIVE_POLICY);
			boolean isFifo = queue.endsWith(".fifo");
			return new QueueAttributes(queueUrl, hasRedrivePolicy, getVisibility(attributes), isFifo);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while fetching attributes for queue " + queue, e);
		}
		catch (ExecutionException e) {
			throw new IllegalStateException("ExecutionException while fetching attributes for queue " + queue, e);
		}
	}

	private static Integer getVisibility(Map<QueueAttributeName, String> attributes) {
		String visibilityTimeout = attributes.get(QueueAttributeName.VISIBILITY_TIMEOUT);
		return visibilityTimeout != null ? Integer.parseInt(visibilityTimeout) : null;
	}

}
