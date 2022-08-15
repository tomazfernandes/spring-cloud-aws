package io.awspring.cloud.sqs.listener.acknowledgement.handler;

import org.junit.jupiter.api.Test;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.verify;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
class AlwaysAcknowledgementHandlerTests extends AbstractAcknowledgementHandlerTests{

	@Test
	void shouldAckOnSuccess() {
		AcknowledgementHandler<String> handler = new AlwaysAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onSuccess(message, callback);
		verify(callback).onAcknowledge(message);
	}

	@Test
	void shouldAckOnError() {
		AcknowledgementHandler<String> handler = new AlwaysAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onError(messages, throwable, callback);
		verify(callback).onAcknowledge(messages);
	}

	@Test
	void shouldAckOnSuccessBatch() {
		AcknowledgementHandler<String> handler = new AlwaysAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onSuccess(messages, callback);
		verify(callback).onAcknowledge(messages);
	}

	@Test
	void shouldAckOnErrorBatch() {
		AcknowledgementHandler<String> handler = new AlwaysAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onError(messages, throwable, callback);
		verify(callback).onAcknowledge(messages);
	}

}
