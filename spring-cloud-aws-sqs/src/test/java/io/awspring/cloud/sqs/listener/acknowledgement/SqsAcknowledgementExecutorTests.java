package io.awspring.cloud.sqs.listener.acknowledgement;


import io.awspring.cloud.sqs.CompletableFutures;
import io.awspring.cloud.sqs.listener.QueueAttributes;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
@ExtendWith(MockitoExtension.class)
class SqsAcknowledgementExecutorTests {

	@Mock
	SqsAsyncClient sqsAsyncClient;

	@Mock
	QueueAttributes queueAttributes;

	@Mock
	Message<String> message;

	@Mock
	MessageHeaders messageHeaders;

	@Mock
	IllegalArgumentException throwable;

	UUID id = UUID.randomUUID();

	String queueName = "queueName";

	String queueUrl = "queueUrl";

	String receiptHandle = "receiptHandle";

	@Test
	void shouldDeleteMessages() throws Exception {
		Collection<Message<String>> messages = Collections.singletonList(message);
		given(message.getHeaders()).willReturn(messageHeaders);
		given(messageHeaders.get(SqsHeaders.SQS_MESSAGE_ID_HEADER, UUID.class)).willReturn(id);
		given(queueAttributes.getQueueName()).willReturn(queueName);
		given(queueAttributes.getQueueUrl()).willReturn(queueUrl);
		given(messageHeaders.get(SqsHeaders.SQS_RECEIPT_HANDLE_HEADER, String.class)).willReturn(receiptHandle);
		given(sqsAsyncClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
			.willReturn(CompletableFuture.completedFuture(null));

		SqsAcknowledgementExecutor<String> executor = new SqsAcknowledgementExecutor<>();
		executor.setSqsAsyncClient(sqsAsyncClient);
		executor.setQueueAttributes(queueAttributes);
		executor.execute(messages).get();

		ArgumentCaptor<DeleteMessageBatchRequest> requestCaptor
			= ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
		verify(sqsAsyncClient).deleteMessageBatch(requestCaptor.capture());
		DeleteMessageBatchRequest request = requestCaptor.getValue();
		assertThat(request.queueUrl()).isEqualTo(queueUrl);
		DeleteMessageBatchRequestEntry entry = request.entries().get(0);
		assertThat(entry.receiptHandle()).isEqualTo(receiptHandle);
	}

	@Test
	void shouldWrapDeletionErrors() {
		Collection<Message<String>> messages = Collections.singletonList(message);
		given(message.getHeaders()).willReturn(messageHeaders);
		given(messageHeaders.get(SqsHeaders.SQS_MESSAGE_ID_HEADER, UUID.class)).willReturn(id);
		given(queueAttributes.getQueueName()).willReturn(queueName);
		given(queueAttributes.getQueueUrl()).willReturn(queueUrl);
		given(messageHeaders.get(SqsHeaders.SQS_RECEIPT_HANDLE_HEADER, String.class)).willReturn(receiptHandle);
		given(sqsAsyncClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
			.willReturn(CompletableFutures.failedFuture(throwable));

		SqsAcknowledgementExecutor<String> executor = new SqsAcknowledgementExecutor<>();
		executor.setSqsAsyncClient(sqsAsyncClient);
		executor.setQueueAttributes(queueAttributes);
		assertThatThrownBy(() -> executor.execute(messages).join())
			.isInstanceOf(CompletionException.class)
			.getCause()
			.isInstanceOf(SqsAcknowledgementException.class)
			.asInstanceOf(type(SqsAcknowledgementException.class))
			.extracting(SqsAcknowledgementException::getFailedAcknowledgements)
			.asList()
			.containsExactly(message);
	}

	@Test
	void shouldWrapIfErrorIsThrown() {
		Collection<Message<String>> messages = Collections.singletonList(message);
		given(message.getHeaders()).willReturn(messageHeaders);
		given(messageHeaders.get(SqsHeaders.SQS_MESSAGE_ID_HEADER, UUID.class)).willReturn(id);
		given(queueAttributes.getQueueName()).willReturn(queueName);
		given(queueAttributes.getQueueUrl()).willReturn(queueUrl);
		given(messageHeaders.get(SqsHeaders.SQS_RECEIPT_HANDLE_HEADER, String.class)).willReturn(receiptHandle);
		given(sqsAsyncClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
			.willThrow(throwable);

		SqsAcknowledgementExecutor<String> executor = new SqsAcknowledgementExecutor<>();
		executor.setSqsAsyncClient(sqsAsyncClient);
		executor.setQueueAttributes(queueAttributes);
		assertThatThrownBy(() -> executor.execute(messages).join())
			.isInstanceOf(CompletionException.class)
			.getCause()
			.isInstanceOf(SqsAcknowledgementException.class)
			.asInstanceOf(type(SqsAcknowledgementException.class))
			.extracting(SqsAcknowledgementException::getQueueUrl)
			.isEqualTo(queueUrl);
	}

}
