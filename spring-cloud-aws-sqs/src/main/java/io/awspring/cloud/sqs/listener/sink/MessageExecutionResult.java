package io.awspring.cloud.sqs.listener.sink;

import org.springframework.messaging.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class MessageExecutionResult {

	public final Collection<Message<?>> failedMessages;

	public final Collection<Message<?>> successfulMessages;

	private MessageExecutionResult(Collection<Message<?>> successfulMessages,
								   Collection<Message<?>> failedMessages) {
		this.successfulMessages = Collections.unmodifiableCollection(successfulMessages);
		this.failedMessages = Collections.unmodifiableCollection(failedMessages);
	}

	public static MessageExecutionResult empty() {
		return new MessageExecutionResult(Collections.emptyList(), Collections.emptyList());
	}

	public static <T> MessageExecutionResult successfulMessages(Collection<Message<T>> messages) {
		return new MessageExecutionResult(Collections.unmodifiableCollection(messages), Collections.emptyList());
	}

	public static <T> MessageExecutionResult failedMessages(Collection<Message<T>> messages) {
		return new MessageExecutionResult(Collections.emptyList(), Collections.unmodifiableCollection(messages));
	}

	public static <T> MessageExecutionResult successfulMessage(Message<T> message) {
		return new MessageExecutionResult(Collections.singletonList(message), Collections.emptyList());
	}

	public static <T> MessageExecutionResult failedMessage(Message<T> message) {
		return new MessageExecutionResult(Collections.emptyList(), Collections.singletonList(message));
	}

	public MessageExecutionResult failed(Message<?> message) {
		List<Message<?>> failed = new ArrayList<>(this.failedMessages);
		failed.add(message);
		return new MessageExecutionResult(this.successfulMessages, Collections.unmodifiableCollection(failed));
	}

	public MessageExecutionResult successful(Message<?> message) {
		List<Message<?>> success = new ArrayList<>(this.successfulMessages);
		success.add(message);
		return new MessageExecutionResult(Collections.unmodifiableCollection(success), successfulMessages);
	}

	public MessageExecutionResult merge(MessageExecutionResult result) {
		Collection<Message<?>> successfulMessages = new ArrayList<>(result.successfulMessages);
		successfulMessages.addAll(this.successfulMessages);
		Collection<Message<?>> failedMessages = new ArrayList<>(result.failedMessages);
		failedMessages.addAll(this.failedMessages);
		return new MessageExecutionResult(successfulMessages, failedMessages);
	}
}
