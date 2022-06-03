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
package io.awspring.cloud.messaging.support.endpoint;

import org.springframework.util.Assert;

import java.util.Collection;

/**
 * Base class for implementing an {@link Endpoint}.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractEndpoint implements Endpoint {

	private final Collection<String> logicalNames;

	private final String listenerContainerFactoryName;

	private final String id;

	protected AbstractEndpoint(Collection<String> logicalNames, String listenerContainerFactoryName, String id) {
		Assert.notEmpty(logicalNames, "logicalNames cannot be empty.");
		Assert.notNull(id, "id cannot be null.");
		this.logicalNames = logicalNames;
		this.listenerContainerFactoryName = listenerContainerFactoryName;
		this.id = id;
	}

	@Override
	public Collection<String> getLogicalNames() {
		return this.logicalNames;
	}

	@Override
	public String getListenerContainerFactoryName() {
		return this.listenerContainerFactoryName;
	}

	@Override
	public String getId() {
		return id;
	}

}
