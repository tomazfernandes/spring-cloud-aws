package io.awspring.cloud.sqs.listener;

import java.time.Duration;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface BackPressureHandler {

	int request(int amount) throws InterruptedException;

	void release(int amount);

	boolean drain(Duration timeout);

}
