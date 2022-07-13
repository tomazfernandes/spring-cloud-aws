package io.awspring.cloud.sqs.listener.pipeline;

import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
interface MessageProcessingPipeline<T> {

	CompletableFuture<Message<T>> process(Message<T> message);

	CompletableFuture<Collection<Message<T>>> process(Collection<Message<T>> messages);

}
