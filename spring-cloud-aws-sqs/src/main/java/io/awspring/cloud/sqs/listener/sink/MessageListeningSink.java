/*
 * Copyright 2013-2022 the original author or authors.
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
package io.awspring.cloud.sqs.listener.sink;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import org.springframework.messaging.Message;

/**
 * Component responsible for deciding how {@link Message} instances should be emitted
 * to the provided {@link AsyncMessageListener}.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface MessageListeningSink<T> {

	/**
	 * Emit the provided {@link Message} instances to the provided {@link AsyncMessageListener}.
	 * @param messages the messages to emit.
	 * @param listener the listener to emit the messages to.
	 * @return a collection of {@link CompletableFuture} instances, each representing the completion signal
	 * of a single message processing.
	 */
	Collection<CompletableFuture<Void>> emit(Collection<Message<T>> messages, AsyncMessageListener<T> listener);

}
