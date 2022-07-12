package io.awspring.cloud.sqs.listener;

import java.time.Duration;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface BackPressureHandler {

	boolean request(int numberOfPermits) throws InterruptedException;

	void release(int numberOfPermits);

	boolean drain(Duration timeout);

}
