package io.awspring.cloud.sqs.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * {@link BackPressureHandler} implementation that uses a {@link Semaphore} for handling
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SemaphoreBackPressureHandler implements BackPressureHandler {

	private static final Logger logger = LoggerFactory.getLogger(SemaphoreBackPressureHandler.class);

	private final Semaphore semaphore;

	private final int totalPermits;

	private final Duration acquireTimeout;

	public SemaphoreBackPressureHandler(int totalPermits, Duration acquireTimeout) {
		this.totalPermits = totalPermits;
		this.acquireTimeout = acquireTimeout;
		this.semaphore = new Semaphore(totalPermits);
	}

	@Override
	public int request(int amount) throws InterruptedException {
		logger.debug("Acquiring {} permits", amount);
		boolean hasAcquired = this.semaphore.tryAcquire(amount,
			this.acquireTimeout.getSeconds(), TimeUnit.SECONDS);
		if (hasAcquired) {
			logger.trace("{} permits acquired", amount);
			logger.trace("Permits left: {}", this.semaphore.availablePermits());
		} else {
			logger.trace("Not able to acquire permits in {} seconds. Skipping.",
				this.acquireTimeout.getSeconds());
		}
		return hasAcquired ? amount : 0;
	}

	@Override
	public void release(int amount) {
		logger.debug("Releasing {} permits", amount);
		this.semaphore.release(amount);
	}

	@Override
	public boolean drain(Duration timeout) {
		logger.debug("Waiting for up to {} seconds for approx. {} permits to be released", timeout,
			totalPermits - this.semaphore.availablePermits());
		try {
			return this.semaphore.tryAcquire(totalPermits, (int) timeout.getSeconds(),
				TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while waiting to acquire permits", e);
		}
	}
}
