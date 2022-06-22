/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.awspring.cloud.sqs.listener.splitter;

import org.springframework.messaging.Message;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Interface for splitting the message batch returned by the
 * {@link io.awspring.cloud.sqs.listener.poller.AsyncMessagePoller}
 * and sending the {@link Message}s to a processing pipeline according
 * to the implementation's strategy.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
@FunctionalInterface
public interface AsyncMessageSplitter<T> {

	/**
	 * Split the message batch and feed the messages to the processing pipeline.
	 * @param messages the message batch.
	 * @param processingPipeline the processing pipeline.
	 * @return a collection of completable futures where each future represents
	 * the processing of one message from the batch.
	 */
	Collection<CompletableFuture<Void>> splitAndProcess(Collection<Message<T>> messages,
														Function<Message<T>, CompletableFuture<Void>> processingPipeline);

}
