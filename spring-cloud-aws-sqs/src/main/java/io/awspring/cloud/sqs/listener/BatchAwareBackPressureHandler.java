package io.awspring.cloud.sqs.listener;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface BatchAwareBackPressureHandler extends BackPressureHandler {

	int requestBatch() throws InterruptedException;

	void releaseBatch();

}
