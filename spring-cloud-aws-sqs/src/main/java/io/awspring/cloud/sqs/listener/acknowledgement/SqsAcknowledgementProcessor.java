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
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class SqsAcknowledgementProcessor<T> extends ThreadWaitingAcknowledgementProcessor<T> implements SqsAsyncClientAware, QueueAttributesAware {

	private static final Logger logger = LoggerFactory.getLogger(SqsAcknowledgementProcessor.class);

    private SqsAsyncClient sqsAsyncClient;
    
    private String queueUrl;

	@Override
	public void setQueueAttributes(QueueAttributes queueAttributes) {
		this.queueUrl = queueAttributes.getQueueUrl();
	}

	@Override
	public void setSqsAsyncClient(SqsAsyncClient sqsAsyncClient) {
		this.sqsAsyncClient = sqsAsyncClient;
	}

	@Override
	protected CompletableFuture<Void> doProcessAcknowledgements(Collection<Message<T>> messagesToAck) {
		try {
			return deleteMessages(messagesToAck);
		}
		catch (Exception e) {
			logger.error("Error acknowleding messages {}", MessageHeaderUtils.getId(messagesToAck), e);
			return CompletableFutures.failedFuture(e);
		}
	}

	private CompletableFuture<Void> deleteMessages(Collection<Message<T>> messagesToAck) {
		if (messagesToAck.size() > 10) {
            // Partition list
        }
		List<String> handlesToAck = messagesToAck
			.stream()
			.map(message -> MessageHeaderUtils.getHeaderAsString(message, SqsHeaders.SQS_RECEIPT_HANDLE_HEADER))
			.collect(Collectors.toList());
		logger.trace("Acknowledging messages: {}", MessageHeaderUtils.getId(messagesToAck));
		return this.sqsAsyncClient.deleteMessageBatch(createDeleteMessageBatchRequest(handlesToAck)).thenRun(() -> {
			})
			.whenComplete((v, t) -> {
				if (t != null) {
					logger.error("Error acknowledging messages {}", MessageHeaderUtils.getId(messagesToAck), t);
				} else {
					logger.trace("Done acknowledging messages: {}", MessageHeaderUtils.getId(messagesToAck));
				}
			});
	}

	private DeleteMessageBatchRequest createDeleteMessageBatchRequest(List<String> handlesToAck) {
		return DeleteMessageBatchRequest
			.builder()
			.queueUrl(this.queueUrl)
			.entries(handlesToAck.stream().map(this::toDeleteMessageEntry).collect(Collectors.toList()))
			.build();
	}

	private DeleteMessageBatchRequestEntry toDeleteMessageEntry(String handle) {
		return DeleteMessageBatchRequestEntry
			.builder()
			.receiptHandle(handle)
			.id(UUID.randomUUID().toString())
			.build();
	}

}
