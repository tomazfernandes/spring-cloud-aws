package io.awspring.cloud.sqs.listener;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public enum PermitAcquiringStrategy {

	WHOLE_BATCHES_ONLY,

	PARTIAL_BATCHES_ENABLED

}
