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
package io.awspring.cloud.sqs.listener.acknowledgement.handler;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import io.awspring.cloud.sqs.CompletableFutures;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementCallback;
import org.springframework.messaging.Message;

/**
 * Interface for managing acknowledgement in success and failure scenarios.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface AcknowledgementHandler<T> {

	/**
	 * Called by the {@link io.awspring.cloud.sqs.listener.MessageListenerContainer} when the {@link Message} is
	 * processed successfully by the {@link io.awspring.cloud.sqs.listener.AsyncMessageListener}.
	 * @param message the message.
	 * @return a completable future.
	 */
	default CompletableFuture<Void> onSuccess(Message<T> message, AcknowledgementCallback<T> callback) {
		return CompletableFuture.completedFuture(null);
	}

	default CompletableFuture<Void> onSuccess(Collection<Message<T>> messages, AcknowledgementCallback<T> callback) {
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * Called by the {@link io.awspring.cloud.sqs.listener.MessageListenerContainer} when the {@link Message} is
	 * processed with an error. by the {@link io.awspring.cloud.sqs.listener.AsyncMessageListener}.
	 * @param message the message.
	 * @param t the error thrown by the listener.
	 * @return a completable future.
	 */
	default CompletableFuture<Void> onError(Message<T> message, Throwable t, AcknowledgementCallback<T> callback) {
		return CompletableFuture.completedFuture(null);
	}

	default CompletableFuture<Void> onError(Collection<Message<T>> messages, Throwable t, AcknowledgementCallback<T> callback) {
		return CompletableFuture.completedFuture(null);
	}

}
