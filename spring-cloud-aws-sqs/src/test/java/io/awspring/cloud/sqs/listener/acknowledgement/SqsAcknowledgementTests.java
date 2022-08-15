package io.awspring.cloud.sqs.listener.acknowledgement;

import io.awspring.cloud.sqs.CompletableFutures;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
class SqsAcknowledgementTests {

	@Mock
	SqsAsyncClient sqsAsyncClient;

	String queueUrl = "queueUrl";

	String receiptHandle = "receiptHandle";

	String messageId = "messageId";

	@Mock
	Throwable throwable;

	@Test
	void shouldAcknowledge() {
		given(sqsAsyncClient.deleteMessage(any(Consumer.class)))
			.willReturn(CompletableFuture.completedFuture(null));

		Acknowledgement acknowledge = new SqsAcknowledgement(sqsAsyncClient, queueUrl, receiptHandle, messageId);
		acknowledge.acknowledge();

		ArgumentCaptor<Consumer<DeleteMessageRequest.Builder>> requestCaptor
			= ArgumentCaptor.forClass(Consumer.class);
		verify(sqsAsyncClient).deleteMessage(requestCaptor.capture());
		Consumer<DeleteMessageRequest.Builder> builderConsumer = requestCaptor.getValue();
		DeleteMessageRequest.Builder builder = DeleteMessageRequest.builder();
		builderConsumer.accept(builder);
		DeleteMessageRequest request = builder.build();

		assertThat(request.queueUrl()).isEqualTo(queueUrl);
		assertThat(request.receiptHandle()).isEqualTo(receiptHandle);

	}

	@Test
	void shouldAcknowledgeAsync() throws Exception {
		given(sqsAsyncClient.deleteMessage(any(Consumer.class)))
			.willReturn(CompletableFuture.completedFuture(null));

		AsyncAcknowledgement acknowledge = new SqsAcknowledgement(sqsAsyncClient, queueUrl, receiptHandle, messageId);
		acknowledge.acknowledgeAsync().get();

		ArgumentCaptor<Consumer<DeleteMessageRequest.Builder>> requestCaptor
			= ArgumentCaptor.forClass(Consumer.class);
		verify(sqsAsyncClient).deleteMessage(requestCaptor.capture());
		Consumer<DeleteMessageRequest.Builder> builderConsumer = requestCaptor.getValue();
		DeleteMessageRequest.Builder builder = DeleteMessageRequest.builder();
		builderConsumer.accept(builder);
		DeleteMessageRequest request = builder.build();

		assertThat(request.queueUrl()).isEqualTo(queueUrl);
		assertThat(request.receiptHandle()).isEqualTo(receiptHandle);

	}

	@Test
	void shouldWrapDeletionError() {
		given(sqsAsyncClient.deleteMessage(any(Consumer.class)))
			.willReturn(CompletableFutures.failedFuture(throwable));

		AsyncAcknowledgement acknowledgement = new SqsAcknowledgement(sqsAsyncClient, queueUrl, receiptHandle, messageId);

		assertThatThrownBy(() -> acknowledgement.acknowledgeAsync().get())
			.isInstanceOf(ExecutionException.class)
			.getCause()
			.isInstanceOf(SqsAcknowledgementException.class)
			.asInstanceOf(InstanceOfAssertFactories.THROWABLE)
			.getCause()
			.isInstanceOf(CompletionException.class)
			.asInstanceOf(InstanceOfAssertFactories.THROWABLE)
			.getCause()
			.isEqualTo(throwable);
	}

	@Test
	void shouldWrapIfErrorIsThrown() {
		given(sqsAsyncClient.deleteMessage(any(Consumer.class)))
			.willReturn(CompletableFutures.failedFuture(throwable));

		Acknowledgement acknowledgement = new SqsAcknowledgement(sqsAsyncClient, queueUrl, receiptHandle, messageId);

		assertThatThrownBy(acknowledgement::acknowledge)
			.isInstanceOf(SqsAcknowledgementException.class)
			.asInstanceOf(InstanceOfAssertFactories.THROWABLE)
			.getCause()
			.isInstanceOf(ExecutionException.class)
			.asInstanceOf(InstanceOfAssertFactories.THROWABLE)
			.getCause()
			.isEqualTo(throwable);
	}

}
