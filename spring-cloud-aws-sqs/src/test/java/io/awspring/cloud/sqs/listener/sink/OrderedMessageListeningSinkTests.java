package io.awspring.cloud.sqs.listener.sink;

import io.awspring.cloud.sqs.listener.MessageProcessingContext;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.pipeline.MessageProcessingPipeline;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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
		int numberOfMessagesToEmit = 1000;
		List<Message<Integer>> messagesToEmit = IntStream.range(0, numberOfMessagesToEmit)
			.mapToObj(index -> MessageBuilder.withPayload(index).setHeader(SqsHeaders.SQS_MESSAGE_ID_HEADER, UUID.randomUUID()).build()).collect(toList());
		List<Message<Integer>> received = new ArrayList<>(numberOfMessagesToEmit);
		AbstractMessageProcessingPipelineSink<Integer> sink = new OrderedMessageSink<>();
		sink.setExecutor(Runnable::run);
		sink.setMessagePipeline(getMessageProcessingPipeline(received));
		sink.start();
		sink.emit(messagesToEmit, MessageProcessingContext.create()).join();
		sink.stop();
		assertThat(received).containsSequence(messagesToEmit);
	}

	private MessageProcessingPipeline<Integer> getMessageProcessingPipeline(List<Message<Integer>> received) {
		return new MessageProcessingPipeline<Integer>() {
			@Override
			public CompletableFuture<Message<Integer>> process(Message<Integer> message, MessageProcessingContext<Integer> context) {
				received.add(message);
				return CompletableFuture.completedFuture(message);
			}
		};
	}

}
