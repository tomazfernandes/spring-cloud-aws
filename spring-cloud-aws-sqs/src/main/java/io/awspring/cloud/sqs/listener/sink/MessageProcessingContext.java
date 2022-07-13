package io.awspring.cloud.sqs.listener.sink;

import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Representation of a processing context that can be used for communication
 * between components.
 * This class is immutable and thread-safe.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class MessageProcessingContext<T> {

	private final Consumer<Message<T>> completionCallback;

	private MessageProcessingContext(Consumer<Message<T>> completionCallback) {
		Assert.notNull(completionCallback, "backPressureHandler cannot be null");
		this.completionCallback = completionCallback;
	}

	public static <T> MessageProcessingContext<T> withCompletionCallback(Consumer<Message<T>> completionCallback) {
		return new MessageProcessingContext<>(completionCallback);
	}

	public void messageProcessingComplete(Message<T> msg) {
		this.completionCallback.accept(msg);
	}

	public void messageProcessingComplete(Collection<Message<T>> msgs) {
		msgs.forEach(this::messageProcessingComplete);
	}

}
