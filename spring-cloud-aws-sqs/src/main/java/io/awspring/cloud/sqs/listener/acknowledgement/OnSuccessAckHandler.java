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
package io.awspring.cloud.sqs.listener.acknowledgement;

import io.awspring.cloud.sqs.MessageHeaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.concurrent.CompletableFuture;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class OnSuccessAckHandler<T> implements AsyncAckHandler<T> {

	private static final Logger logger = LoggerFactory.getLogger(OnSuccessAckHandler.class);

	@Override
	public CompletableFuture<Void> onSuccess(Message<T> message) {
		logger.trace("Acknowledging message {}", MessageHeaderUtils.getId(message));
		return MessageHeaderUtils.getAcknowledgement(message)
			.acknowledge()
			.thenRun(() -> logger.trace("Message {} acknowledged.", MessageHeaderUtils.getId(message)))
			.exceptionally(t -> logError(message, t));
	}

	private Void logError(Message<T> message, Throwable t) {
		logger.error("Error acknowledging message {}", MessageHeaderUtils.getId(message), t);
		return null;
	}
}
