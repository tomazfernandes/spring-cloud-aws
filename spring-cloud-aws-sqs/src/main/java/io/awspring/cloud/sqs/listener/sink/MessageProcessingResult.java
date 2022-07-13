package io.awspring.cloud.sqs.listener.sink;

import org.springframework.messaging.Message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Representation of the processing result for an individual or
 * collection of {@link Message}s.
 *
 * This class is immutable and thread-safe.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class MessageProcessingResult {

	public final Collection<Message<?>> failedMessages;

	public final Collection<Message<?>> successfulMessages;

	private MessageProcessingResult(Collection<Message<?>> successfulMessages,
									Collection<Message<?>> failedMessages) {
		this.successfulMessages = Collections.unmodifiableCollection(successfulMessages);
		this.failedMessages = Collections.unmodifiableCollection(failedMessages);
	}

	public static MessageProcessingResult empty() {
		return new MessageProcessingResult(Collections.emptyList(), Collections.emptyList());
	}

	public static <T> MessageProcessingResult successfulMessages(Collection<Message<T>> messages) {
		return new MessageProcessingResult(Collections.unmodifiableCollection(messages), Collections.emptyList());
	}

	public static <T> MessageProcessingResult failedMessages(Collection<Message<T>> messages) {
		return new MessageProcessingResult(Collections.emptyList(), Collections.unmodifiableCollection(messages));
	}

	public static <T> MessageProcessingResult successfulMessage(Message<T> message) {
		return new MessageProcessingResult(Collections.singletonList(message), Collections.emptyList());
	}

	public static <T> MessageProcessingResult failedMessage(Message<T> message) {
		return new MessageProcessingResult(Collections.emptyList(), Collections.singletonList(message));
	}

	public MessageProcessingResult failed(Message<?> message) {
		List<Message<?>> failed = new ArrayList<>(this.failedMessages);
		failed.add(message);
		return new MessageProcessingResult(this.successfulMessages, Collections.unmodifiableCollection(failed));
	}

	public MessageProcessingResult successful(Message<?> message) {
		List<Message<?>> success = new ArrayList<>(this.successfulMessages);
		success.add(message);
		return new MessageProcessingResult(Collections.unmodifiableCollection(success), successfulMessages);
	}

	public MessageProcessingResult merge(MessageProcessingResult result) {
		Collection<Message<?>> successfulMessages = new ArrayList<>(result.successfulMessages);
		successfulMessages.addAll(this.successfulMessages);
		Collection<Message<?>> failedMessages = new ArrayList<>(result.failedMessages);
		failedMessages.addAll(this.failedMessages);
		return new MessageProcessingResult(successfulMessages, failedMessages);
	}
}
