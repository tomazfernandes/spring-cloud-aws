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
package io.awspring.cloud.sqs;

import io.awspring.cloud.sqs.listener.SqsMessageHeaders;
import io.awspring.cloud.sqs.listener.acknowledgement.AsyncAcknowledgement;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.Objects;
import java.util.UUID;

/**
 * Utility class for extracting {@link MessageHeaders} from a {@link Message}.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class MessageHeaderUtils {

	private MessageHeaderUtils() {
	}

	public static String getId(Message<?> message) {
		return Objects.requireNonNull(message.getHeaders().get(MessageHeaders.ID, UUID.class),
			() -> "No ID found for message " + message).toString();
	}

	public static AsyncAcknowledgement getAcknowledgement(Message<?> message) {
		return Objects.requireNonNull(message.getHeaders().get(SqsMessageHeaders.ACKNOWLEDGMENT_HEADER, AsyncAcknowledgement.class),
			() -> "No Acknowledgment found for message " + message);
	}

}
