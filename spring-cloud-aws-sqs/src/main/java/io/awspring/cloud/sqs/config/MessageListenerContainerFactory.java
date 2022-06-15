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
package io.awspring.cloud.sqs.config;

import io.awspring.cloud.messaging.support.listener.MessageListenerContainer;

/**
 * Creates a {@link MessageListenerContainer} instance for a given
 * {@link Endpoint} or endpoint names.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
@FunctionalInterface
public interface MessageListenerContainerFactory<C extends MessageListenerContainer<?>, E extends Endpoint> {

	C createContainerInstance(String... endpointNames);

	default C createContainerInstance(E endpoint) {
		throw new UnsupportedOperationException("Not implemented");
	}
}
