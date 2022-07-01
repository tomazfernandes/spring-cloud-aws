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

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

/**
 * {@link MessageListeningSink} implementation that processes all messages from the provided batch in parallel.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class FanOutMessageSink<T> extends AbstractMessageListeningSink<T> {

	Logger logger = LoggerFactory.getLogger(FanOutMessageSink.class);

	@Override
	protected Collection<CompletableFuture<Integer>> doEmit(Collection<Message<T>> messages,
			AsyncMessageListener<T> messageListener) {
		logger.trace("Splitting {} messages", messages.size());
		return messages.stream().map(msg -> execute(() -> messageListener.onMessage(msg), 1))
				.collect(Collectors.toList());
	}
}
