package io.awspring.cloud.sqs.operations;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/**
 * Options for receiving messages from SQS queues, with a method chaining API.
 * @param <T> the payload type.
 * @param <O> the implementation class to be returned by the chained methods.
 */
public interface SqsReceiveOptions<T, O extends SqsReceiveOptions<T, O>> {

	/**
	 * Set the queue from which to receive messages from.
	 * @param queue the queue name.
	 * @return the options instance.
	 */
	O queue(String queue);

	/**
	 * Set the maximum amount of time to wait for messages in the queue
	 * before returning with less than maximum of messages or empty.
	 * @param pollTimeout the amount of time.
	 * @return the options instance.
	 */
	O pollTimeout(Duration pollTimeout);

	/**
	 * Set the class to which the payload should be converted to.
	 * @param payloadClass the class.
	 * @return the options instance.
	 */
	O payloadClass(Class<T> payloadClass);

	/**
	 * Set the visibility timeout to be applied by received messages.
	 * @param visibilityTimeout the timeout.
	 * @return the options instance.
	 */
	O visibilityTimeout(Duration visibilityTimeout);

	/**
	 * Provide a header name and value to be added to returned messages.
	 * @param name the header name.
	 * @param value the header value.
	 * @return the options instance.
	 */
	O additionalHeader(String name, Object value);

	/**
	 * Provide headers to be added to returned messages.
	 * @param headers the headers to add.
	 * @return the options instance.
	 */
	O additionalHeaders(Map<String, Object> headers);

	/**
	 * Set the maximum number of messages to be returned.
	 * @param maxNumberOfMessages the number of messages.
	 * @return the options instance.
	 */
	O maxNumberOfMessages(Integer maxNumberOfMessages);

	/**
	 * Specific options for Standard Sqs queues
	 * @param <T> the payload type.
	 */
	interface Standard<T> extends SqsReceiveOptions<T, Standard<T>> {
	}

	/**
	 * Specific options for Fifo Sqs queues.
	 * @param <T> the payload type.
	 */
	interface Fifo<T> extends SqsReceiveOptions<T, Fifo<T>> {

		/**
		 * Set the receiveRequestAttemptId required attribute.
		 * If none is provided a random one is generated.
		 * @param receiveRequestAttemptId the id.
		 * @return the options instance.
		 */
		Fifo<T> receiveRequestAttemptId(UUID receiveRequestAttemptId);

	}

}
