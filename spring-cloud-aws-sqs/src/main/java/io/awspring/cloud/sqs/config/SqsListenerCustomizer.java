package io.awspring.cloud.sqs.config;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
@FunctionalInterface
public interface SqsListenerCustomizer {

	void configure(EndpointRegistrar registrar);

}
