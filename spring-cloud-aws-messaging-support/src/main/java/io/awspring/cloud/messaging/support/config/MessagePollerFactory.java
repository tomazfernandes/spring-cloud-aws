package io.awspring.cloud.messaging.support.config;

import io.awspring.cloud.messaging.support.listener.AsyncMessagePoller;

import java.util.Collection;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface MessagePollerFactory<T> {

	Collection<AsyncMessagePoller<T>> create(Collection<String> endpointNames);

}
