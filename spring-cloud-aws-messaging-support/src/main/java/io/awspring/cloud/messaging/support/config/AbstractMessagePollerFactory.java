package io.awspring.cloud.messaging.support.config;

import io.awspring.cloud.messaging.support.listener.AsyncMessagePoller;

import java.util.Collection;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractMessagePollerFactory<T> implements MessagePollerFactory<T> {

	public Collection<AsyncMessagePoller<T>> create(Collection<String> endpointNames) {
		return doCreateProducers(endpointNames);
	}

	protected abstract Collection<AsyncMessagePoller<T>> doCreateProducers(Collection<String> endpointNames);
}
