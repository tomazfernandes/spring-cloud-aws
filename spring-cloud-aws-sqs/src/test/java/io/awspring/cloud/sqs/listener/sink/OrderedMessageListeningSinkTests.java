package io.awspring.cloud.sqs.listener.sink;

import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
class OrderedMessageListeningSinkTests {

	@Test
	void shouldEmitInOrder() {
		List<Message<Integer>> messagesToEmit = IntStream.range(0, 100000)
			.mapToObj(index -> MessageBuilder.withPayload(index).build()).collect(toList());
		List<Message<Integer>> received = new ArrayList<>(100000);
		AbstractMessageListeningSink<Integer> sink = new OrderedMessageListeningSink<>();
		sink.setTaskExecutor(Runnable::run);
		sink.setMessageListener(msg -> {
			received.add(msg);
			return CompletableFuture.completedFuture(null);
		});
		sink.start();
		sink.emit(messagesToEmit, MessageProcessingContext.withCompletionCallback(msg -> {})).join();
		sink.stop();
		assertThat(received).containsSequence(messagesToEmit);
	}

}
