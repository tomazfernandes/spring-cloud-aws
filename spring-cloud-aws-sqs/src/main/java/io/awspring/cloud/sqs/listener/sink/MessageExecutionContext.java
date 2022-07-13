package io.awspring.cloud.sqs.listener.sink;

import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class MessageExecutionContext<T> {

	private final Consumer<Message<T>> completionCallback;

	private MessageExecutionContext(Consumer<Message<T>> completionCallback) {
		Assert.notNull(completionCallback, "backPressureHandler cannot be null");
		this.completionCallback = completionCallback;
	}

	public static <T> MessageExecutionContext<T> withCompletionCallback(Consumer<Message<T>> completionCallback) {
		return new MessageExecutionContext<>(completionCallback);
	}

	public void messageProcessingComplete(Message<T> msg) {
		this.completionCallback.accept(msg);
	}

	public void messageProcessingComplete(Collection<Message<T>> msgs) {
		msgs.forEach(this::messageProcessingComplete);
	}

}
