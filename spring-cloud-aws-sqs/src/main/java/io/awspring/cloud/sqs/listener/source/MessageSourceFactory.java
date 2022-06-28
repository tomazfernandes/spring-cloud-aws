package io.awspring.cloud.sqs.listener.source;

import io.awspring.cloud.sqs.listener.ContainerOptions;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface MessageSourceFactory<T> {

	MessageSource<T> create(String endpointName);

}
