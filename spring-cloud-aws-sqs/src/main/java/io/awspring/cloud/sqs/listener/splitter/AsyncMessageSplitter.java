package io.awspring.cloud.sqs.listener.splitter;

import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@FunctionalInterface
public interface AsyncMessageSplitter<T> {

	Collection<CompletableFuture<Void>> splitAndProcess(Collection<Message<T>> messages,
														Function<Message<T>, CompletableFuture<Void>> processingPipeline);

}
