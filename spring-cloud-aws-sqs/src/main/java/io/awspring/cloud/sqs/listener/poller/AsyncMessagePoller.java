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
package io.awspring.cloud.sqs.listener.poller;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.springframework.messaging.Message;

/**
 * Interface for polling a resource and returning {@link Message} instances.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
@FunctionalInterface
public interface AsyncMessagePoller<T> {

	/**
	 * Polls for the specified amount of messages for up to the specified duration.
	 * @param numberOfMessages the maximum number of messages returned by the poll.
	 * @param timeout the maximum amount of time to poll for messages.
	 * @return a completable future containing the batch of polled messages.
	 */
	CompletableFuture<Collection<Message<T>>> poll(int numberOfMessages, Duration timeout);

}
