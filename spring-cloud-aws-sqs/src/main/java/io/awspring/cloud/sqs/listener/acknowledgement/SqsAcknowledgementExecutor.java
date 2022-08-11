package io.awspring.cloud.sqs.listener.acknowledgement;

import io.awspring.cloud.sqs.CompletableFutures;
import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.listener.QueueAttributes;
import io.awspring.cloud.sqs.listener.QueueAttributesAware;
import io.awspring.cloud.sqs.listener.SqsAsyncClientAware;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.StopWatch;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class SqsAcknowledgementExecutor<T> implements AcknowledgementExecutor<T>, SqsAsyncClientAware, QueueAttributesAware {

	private static final Logger logger = LoggerFactory.getLogger(SqsAcknowledgementExecutor.class);

    private SqsAsyncClient sqsAsyncClient;
    
    private String queueUrl;

	private String queueName;

	@Override
	public void setQueueAttributes(QueueAttributes queueAttributes) {
		this.queueUrl = queueAttributes.getQueueUrl();
		this.queueName = queueAttributes.getQueueName();
	}

	@Override
	public void setSqsAsyncClient(SqsAsyncClient sqsAsyncClient) {
		this.sqsAsyncClient = sqsAsyncClient;
	}

	@Override
	public CompletableFuture<Void> execute(Collection<Message<T>> messagesToAck) {
		try {
			logger.debug("Executing acknowledgement for {} messages", messagesToAck.size());
			Assert.notEmpty(messagesToAck, () -> "empty collection sent to acknowledge in queue " + this.queueName);
			return deleteMessages(messagesToAck);
		}
		catch (Exception e) {
			return CompletableFutures.failedFuture(createAcknowledgementException(messagesToAck, e));
		}
	}

	private SqsAcknowledgementException createAcknowledgementException(Collection<Message<T>> messagesToAck, Throwable e) {
		return new SqsAcknowledgementException("Error acknowledging messages " + MessageHeaderUtils.getId(messagesToAck),
			messagesToAck, this.queueUrl, e);
	}

	private CompletableFuture<Void> deleteMessages(Collection<Message<T>> messagesToAck) {
		logger.trace("Acknowledging messages for queue {}: {}", this.queueName, MessageHeaderUtils.getId(messagesToAck));
		StopWatch watch = new StopWatch();
		watch.start();
		return CompletableFutures.exceptionallyCompose(this.sqsAsyncClient
			.deleteMessageBatch(createDeleteMessageBatchRequest(messagesToAck))
			.thenRun(() -> {}),
				t -> CompletableFutures.failedFuture(createAcknowledgementException(messagesToAck, t)))
			.whenComplete((v, t) -> logAckResult(messagesToAck, t, watch));
	}

	private DeleteMessageBatchRequest createDeleteMessageBatchRequest(Collection<Message<T>> messagesToAck) {
		return DeleteMessageBatchRequest
			.builder()
			.queueUrl(this.queueUrl)
			.entries(messagesToAck.stream().map(this::toDeleteMessageEntry).collect(Collectors.toList()))
			.build();
	}

	private DeleteMessageBatchRequestEntry toDeleteMessageEntry(Message<T> message) {
		return DeleteMessageBatchRequestEntry
			.builder()
			.receiptHandle(MessageHeaderUtils.getHeaderAsString(message, SqsHeaders.SQS_RECEIPT_HANDLE_HEADER))
			.id(UUID.randomUUID().toString())
			.build();
	}

	private void logAckResult(Collection<Message<T>> messagesToAck, Throwable t, StopWatch watch) {
		watch.stop();
		long totalTimeMillis = watch.getTotalTimeMillis();
		if (totalTimeMillis > 1000) {
			logger.warn("Acknowledgement operation took {} seconds to finish in queue {} for messages {}", totalTimeMillis, this.queueName, MessageHeaderUtils.getId(messagesToAck));
		}
		if (t != null) {
			logger.error("Error acknowledging in queue {} messages {} in {}ms", this.queueName, MessageHeaderUtils.getId(messagesToAck), totalTimeMillis, t);
		}
		else {
			logger.trace("Done acknowledging in queue {} messages: {} in {}ms", this.queueName, MessageHeaderUtils.getId(messagesToAck), totalTimeMillis);
		}
	}

}
