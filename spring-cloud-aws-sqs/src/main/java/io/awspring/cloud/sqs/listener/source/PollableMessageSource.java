package io.awspring.cloud.sqs.listener.source;

import org.springframework.context.SmartLifecycle;

import java.time.Duration;

public interface PollableMessageSource<T> extends MessageSource<T>, SmartLifecycle {

	void setNumberOfMessagesPerPoll(int numberOfMessagesPerPoll);

	void setPollTimeout(Duration pollTimeout);

}
