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
package io.awspring.cloud.sqs.listener.splitter;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

/**
 * {@link AsyncMessageSplitter} implementation that processes provided messages sequentially and in order.
 *
 * @param <T> the {@link Message} payload type.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class OrderedSplitter<T> extends AbstractMessageSplitter<T> {

	Logger logger = LoggerFactory.getLogger(OrderedSplitter.class);

	@Override
	protected Collection<CompletableFuture<Void>> doSplitAndProcessMessages(Collection<Message<T>> messages,
			Function<Message<T>, CompletableFuture<Void>> processingPipeline) {
		logger.debug("Splitting {} messages", messages.size());
		CompletableFuture<Void> identity = new CompletableFuture<>();
		identity.complete(null);
		CompletableFuture.supplyAsync(() -> messages.stream().reduce(identity,
				(voidFuture, msg) -> voidFuture.thenCompose(theVoid -> processingPipeline.apply(msg)), (a, b) -> b),
				getTaskExecutor()).join();
		return super.returnCompletedVoidFutures(messages);
	}

}
