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
package io.awspring.cloud.sqs.listener.sink.adapter;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import io.awspring.cloud.sqs.listener.sink.AbstractMessageListeningSink;
import io.awspring.cloud.sqs.listener.sink.MessageListeningSink;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import io.awspring.cloud.sqs.listener.sink.TaskExecutorAwareComponent;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.Assert;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractDelegatingMessageListeningSinkAdapter<T> implements MessageListeningSink<T>, TaskExecutorAwareComponent {

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
