package io.awspring.cloud.sqs.listener.acknowledgement.handler;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
class OnSuccessAcknowledgementHandlerTests extends AbstractAcknowledgementHandlerTests {

	@Test
	void shouldAckOnSuccess() {
		AcknowledgementHandler<String> handler = new OnSuccessAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onSuccess(message, callback);
		verify(callback).onAcknowledge(message);
	}

	@Test
	void shouldNotAckOnError() {
		AcknowledgementHandler<String> handler = new OnSuccessAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onError(messages, throwable, callback);
		verify(callback, never()).onAcknowledge(messages);
	}

	@Test
	void shouldAckOnSuccessBatch() {
		AcknowledgementHandler<String> handler = new OnSuccessAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onSuccess(messages, callback);
		verify(callback).onAcknowledge(messages);
	}

	@Test
	void shouldNotAckOnErrorBatch() {
		AcknowledgementHandler<String> handler = new OnSuccessAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onError(messages, throwable, callback);
		verify(callback, never()).onAcknowledge(messages);
	}

}
