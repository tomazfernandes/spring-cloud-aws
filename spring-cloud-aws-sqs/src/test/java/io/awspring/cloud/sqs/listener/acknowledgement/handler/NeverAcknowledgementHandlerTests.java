package io.awspring.cloud.sqs.listener.acknowledgement.handler;


import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
class NeverAcknowledgementHandlerTests extends AbstractAcknowledgementHandlerTests{

	@Test
	void shouldNotAckOnSuccess() {
		AcknowledgementHandler<String> handler = new NeverAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onSuccess(message, callback);
		verify(callback, never()).onAcknowledge(message);
	}

	@Test
	void shouldNotAckOnError() {
		AcknowledgementHandler<String> handler = new NeverAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onError(messages, throwable, callback);
		verify(callback, never()).onAcknowledge(messages);
	}

	@Test
	void shouldNotAckOnSuccessBatch() {
		AcknowledgementHandler<String> handler = new NeverAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onSuccess(messages, callback);
		verify(callback, never()).onAcknowledge(messages);
	}

	@Test
	void shouldNotAckOnErrorBatch() {
		AcknowledgementHandler<String> handler = new NeverAcknowledgementHandler<>();
		CompletableFuture<Void> result = handler.onError(messages, throwable, callback);
		verify(callback, never()).onAcknowledge(messages);
	}

}
