package io.awspring.cloud.sqs.config;

import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface SqsAsyncClientSupplier {

	SqsAsyncClient createNewInstance();

}
