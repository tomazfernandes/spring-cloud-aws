package io.awspring.cloud.sqs.listener.sink.adapter;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import io.awspring.cloud.sqs.listener.sink.AbstractMessageListeningSink;
import io.awspring.cloud.sqs.listener.sink.MessageListeningSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.sink.TaskExecutorAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.Assert;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractDelegatingMessageListeningSinkAdapter<T> implements MessageListeningSink<T>, TaskExecutorAware {

	private final MessageSink<T> delegate;

	protected AbstractDelegatingMessageListeningSinkAdapter(MessageSink<T> delegate) {
		Assert.notNull(delegate, "delegate cannot be null");
		this.delegate = delegate;
	}

	@Override
	public void setMessageListener(AsyncMessageListener<T> listener) {
		if (this.delegate instanceof MessageListeningSink) {
			((MessageListeningSink<T>) this.delegate).setMessageListener(listener);
		}
	}

	@Override
	public void setTaskExecutor(TaskExecutor taskExecutor) {
		if (this.delegate instanceof AbstractMessageListeningSink) {
			((AbstractMessageListeningSink<T>) this.delegate).setTaskExecutor(taskExecutor);
		}
	}

	@Override
	public void start() {
		if (this.delegate instanceof SmartLifecycle) {
			((SmartLifecycle) this.delegate).start();
		}
	}

	@Override
	public void stop() {
		if (this.delegate instanceof SmartLifecycle) {
			((SmartLifecycle) this.delegate).stop();
		}
	}

	@Override
	public boolean isRunning() {
		if (this.delegate instanceof SmartLifecycle) {
			return ((SmartLifecycle) this.delegate).isRunning();
		}
		return true;
	}

	protected MessageSink<T> getDelegate() {
		return this.delegate;
	}
}
