package io.awspring.cloud.sqs.listener.pipeline;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;
import io.awspring.cloud.sqs.listener.acknowledgement.AckHandler;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.interceptor.MessageVisibilityExtenderInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.concurrent.Semaphore;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class MessageProcessingContext<T> {

	private static final Logger logger = LoggerFactory.getLogger(MessageVisibilityExtenderInterceptor.class);

	private final Collection<AsyncMessageInterceptor<T>> messageInterceptors;

	private final AsyncMessageListener<T> messageListener;

	private final AsyncErrorHandler<T> errorHandler;

	private final AckHandler<T> ackHandler;

	private final Semaphore semaphore;

	public MessageProcessingContext(Collection<AsyncMessageInterceptor<T>> messageInterceptors,
									AsyncMessageListener<T> messageListener, AsyncErrorHandler<T> errorHandler,
									AckHandler<T> ackHandler, Semaphore semaphore) {
		this.messageInterceptors = messageInterceptors;
		this.messageListener = messageListener;
		this.errorHandler = errorHandler;
		this.ackHandler = ackHandler;
		this.semaphore = semaphore;
	}

	public static <T> MessageProcessingContext.Builder<T> builder() {
		return new Builder<>();
	}

	public Collection<AsyncMessageInterceptor<T>> getMessageInterceptors() {
		return this.messageInterceptors;
	}

	public AsyncMessageListener<T> getMessageListener() {
		return this.messageListener;
	}

	public AsyncErrorHandler<T> getErrorHandler() {
		return this.errorHandler;
	}

	public AckHandler<T> getAckHandler() {
		return this.ackHandler;
	}

	public Semaphore getSemaphore() {
		return this.semaphore;
	}

	public static class Builder<T> {

		private Collection<AsyncMessageInterceptor<T>> messageInterceptors;
		private AsyncMessageListener<T> messageListener;
		private AsyncErrorHandler<T> errorHandler;
		private AckHandler<T> ackHandler;
		private Semaphore semaphore;

		public Builder<T> interceptors(Collection<AsyncMessageInterceptor<T>> messageInterceptors) {
			this.messageInterceptors = messageInterceptors;
			return this;
		}

		public Builder<T> messageListener(AsyncMessageListener<T> messageListener) {
			this.messageListener = messageListener;
			return this;
		}

		public Builder<T> errorHandler(AsyncErrorHandler<T> errorHandler) {
			this.errorHandler = errorHandler;
			return this;
		}

		public Builder<T> ackHandler(AckHandler<T> ackHandler) {
			this.ackHandler = ackHandler;
			return this;
		}

		public Builder<T> semaphore(Semaphore semaphore) {
			this.semaphore = semaphore;
			return this;
		}

		public MessageProcessingContext<T> build() {
			Assert.notNull(this.messageListener, "messageListener cannot be null provided");
			Assert.notNull(this.errorHandler, "No error handler provided");
			Assert.notNull(this.ackHandler, "No ackHandler provided");
			Assert.notNull(this.messageInterceptors, "messageInterceptors cannot be null");
			Assert.notNull(this.semaphore, "semaphore cannot be null");
			return new MessageProcessingContext<>(this.messageInterceptors, this.messageListener,
				this.errorHandler, this.ackHandler, this.semaphore);
		}
	}

}
