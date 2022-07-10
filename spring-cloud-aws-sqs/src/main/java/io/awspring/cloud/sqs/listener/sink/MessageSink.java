package io.awspring.cloud.sqs.listener.sink;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface MessageSink<T> {

	/**
	 * Emit the provided {@link Message} instances to the provided {@link AsyncMessageListener}.
	 * @param messages the messages to emit.
	 * @return a collection of {@link CompletableFuture} instances, each representing the completion signal of a single
	 * message processing.
	 */
	CompletableFuture<Void> emit(Collection<Message<T>> messages);

}
