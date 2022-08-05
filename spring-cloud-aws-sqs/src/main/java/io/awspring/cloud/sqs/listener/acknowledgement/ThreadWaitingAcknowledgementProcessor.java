/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.awspring.cloud.sqs.listener.acknowledgement;

import io.awspring.cloud.sqs.CompletableFutures;
import io.awspring.cloud.sqs.MessageHeaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class ThreadWaitingAcknowledgementProcessor<T> implements AcknowledgementProcessor<T>, AcknowledgementCallback<T> {

	private static final Logger logger = LoggerFactory.getLogger(ThreadWaitingAcknowledgementProcessor.class);

	private final Object waitingMonitor = new Object();

	private final Object lifecycleMonitor = new Object();

	private final WaitingAcknowledgementProcessor acknowledgementProcessor = new WaitingAcknowledgementProcessor();

	private boolean isImmediateAck;

	private BlockingQueue<Message<T>> acks;

	private Integer ackThreshold;

	private Duration ackInterval;

	private volatile boolean isRunning;

	private AcknowledgementOrdering acknowledgementOrdering;

	public void setAcknowledgementInterval(Duration ackInterval) {
		Assert.notNull(ackInterval, "ackInterval canont be null");
		this.ackInterval = ackInterval;
	}

	public void setAcknowledgementThreshold(Integer ackThreshold) {
		Assert.notNull(ackThreshold, "ackThreshold canont be null");
		this.ackThreshold = ackThreshold;
	}

	public void setAcknowledgementOrdering(AcknowledgementOrdering acknowledgementOrdering) {
		Assert.notNull(acknowledgementOrdering, "acknowledgementOrdering canont be null");
		this.acknowledgementOrdering = acknowledgementOrdering;
	}

	@Override
	public AcknowledgementCallback<T> getAcknowledgementCallback() {
		return this;
	}

	@Override
	public CompletableFuture<Void> onAcknowledge(Message<T> message) {
		if (!this.isRunning) {
			logger.warn("AcknowledgementProcessor not running");
			return CompletableFuture.completedFuture(null);
		}
        if (this.isImmediateAck) {
            return this.acknowledgementProcessor.processAcknowledgements(Collections.singletonList(message));
        }
		if (!this.acks.offer(message)) {
			logger.warn("Acknowledgement queue full, dropping acknowledgement for message {}", MessageHeaderUtils.getId(message));
		}
		logger.trace("Received messages {} to ack. Queue size: {}", MessageHeaderUtils.getId(message), this.acks.size());
		synchronized (this.waitingMonitor) {
			this.waitingMonitor.notify();
		}
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public CompletableFuture<Void> onAcknowledge(Collection<Message<T>> messages) {
		if (!this.isRunning) {
			logger.warn("AcknowledgementProcessor not running");
			return CompletableFuture.completedFuture(null);
		}
		if (this.isImmediateAck) {
			return this.acknowledgementProcessor.processAcknowledgements(messages);
		}
		messages.forEach(this::onAcknowledge);
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void start() {
		synchronized (this.lifecycleMonitor) {
			logger.debug("Starting acknowledgement processor");
			this.isRunning = true;
			this.acks = new LinkedBlockingQueue<>();
			this.isImmediateAck = this.ackInterval.equals(Duration.ZERO) && this.ackThreshold == 0;
			new SimpleAsyncTaskExecutor().execute(this.acknowledgementProcessor);
		}
	}

	@Override
	public void stop() {
		synchronized (this.lifecycleMonitor) {
			logger.debug("Stopping acknowledgement processor");
			while (!this.acknowledgementProcessor.ackFutures.isEmpty()) {
				// TODO: Add a timeout
				logger.debug("Waiting for {} tasks to finish", this.acknowledgementProcessor.ackFutures);
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			this.isRunning = false;
			logger.debug("Acknowledgement processor stopped");
		}
	}

	@Override
	public boolean isRunning() {
		return this.isRunning;
	}

	private class WaitingAcknowledgementProcessor implements Runnable {

		private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

		private volatile Instant lastAcknowledgement = Instant.now();

		private final Collection<CompletableFuture<Void>> ackFutures = Collections.synchronizedSet(new HashSet<>());

		private CompletableFuture<Void> lastAckFuture = CompletableFuture.completedFuture(null);

		@Override
		public void run() {
			logger.debug("Starting ack processor");
			this.scheduledExecutorService.scheduleAtFixedRate(() -> {
				synchronized (ThreadWaitingAcknowledgementProcessor.this.waitingMonitor) {
					try {
						logger.trace("Running scheduled thread");
						ThreadWaitingAcknowledgementProcessor.this.waitingMonitor.notify();
						logger.trace("Scheduled thread finished");
					}
					catch (Exception e) {
						logger.error("Error in scheduled thread", e);
					}
				}
			}, 0, 1, TimeUnit.SECONDS);
			while (ThreadWaitingAcknowledgementProcessor.this.isRunning) {
				try {
					Instant now = Instant.now();
					boolean isTimeElapsed = now.isAfter(this.lastAcknowledgement.plus(ThreadWaitingAcknowledgementProcessor.this.ackInterval));
					int currentQueueSize = ThreadWaitingAcknowledgementProcessor.this.acks.size();
					logger.trace("Queue size: {} now: {} lastAcknowledgement: {} isTimeElapsed: {}", currentQueueSize, now, this.lastAcknowledgement, isTimeElapsed);
					if (currentQueueSize > 0 && (isTimeElapsed || currentQueueSize >= ThreadWaitingAcknowledgementProcessor.this.ackThreshold)) {
						int numberOfMessagesToPoll = isTimeElapsed ? Math.min(currentQueueSize, 10) : ThreadWaitingAcknowledgementProcessor.this.ackThreshold;
						logger.trace("Polling {} messages from queue. Queue size: {}", numberOfMessagesToPoll, currentQueueSize);
						List<Message<T>> messagesToAck = pollMessagesToAck(numberOfMessagesToPoll);
						processAcknowledgements(messagesToAck);
						this.lastAcknowledgement = Instant.now();
					}
					else {
						int waitTimeoutMillis = 1000;
						logger.trace("Waiting on monitor for {}ms", waitTimeoutMillis);
						synchronized (ThreadWaitingAcknowledgementProcessor.this.waitingMonitor) {
							ThreadWaitingAcknowledgementProcessor.this.waitingMonitor.wait(waitTimeoutMillis);
						}
						logger.trace("Thread awakened");
					}
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException("Interrupted while waiting for acks", e);
				}
				catch (Exception e) {
					logger.error("Error while handling acknowledgements", e);
				}
			}
			logger.debug("Ack thread stopped");
			this.scheduledExecutorService.shutdownNow();
		}

		private final Object orderingMonitor = new Object();

		public CompletableFuture<Void> processAcknowledgements(Collection<Message<T>> messagesToAck) {
			try {
				if (AcknowledgementOrdering.PARALLEL.equals(ThreadWaitingAcknowledgementProcessor.this.acknowledgementOrdering)) {
					return manageFuture(doProcessAcknowledgements(messagesToAck));
				}
				else {
					if (ThreadWaitingAcknowledgementProcessor.this.isImmediateAck) {
						synchronized (this.orderingMonitor) {
							return processOrderedAcknowledgement(messagesToAck);
						}
					}
					else {
						return processOrderedAcknowledgement(messagesToAck);
					}
				}
			} catch (Exception e) {
				return CompletableFutures.failedFuture(e);
			}
		}

		private CompletableFuture<Void> processOrderedAcknowledgement(Collection<Message<T>> messagesToAck) {
			this.lastAckFuture = this.lastAckFuture.thenRun(() -> manageFuture(doProcessAcknowledgements(messagesToAck)));
			return this.lastAckFuture;
		}

		private CompletableFuture<Void> manageFuture(CompletableFuture<Void> future) {
			this.ackFutures.add(future);
			future.whenComplete((v, t) -> ackFutures.remove(future));
			return future;
		}

	}

	private List<Message<T>> pollMessagesToAck(int numberOfMessagesToPoll) {
		return IntStream
			.range(0, numberOfMessagesToPoll)
			.mapToObj(index -> getPoll())
			.collect(Collectors.toList());
	}

	private Message<T> getPoll() {
		Message<T> poll = this.acks.poll();
		if (poll == null) {
			logger.warn("Poll returned null. Queue size: {}", this.acks.size());
		}
		return poll;
	}

	protected abstract CompletableFuture<Void> doProcessAcknowledgements(Collection<Message<T>> messagesToAck);

}
