package io.awspring.cloud.messaging.support.config;

import io.awspring.cloud.messaging.support.listener.AsyncMessageListener;
import io.awspring.cloud.messaging.support.listener.adapter.AsyncSingleMessageListenerAdapter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.util.Assert;

import java.lang.reflect.Method;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class DefaultMessageListenerFactory<T> implements MessageListenerFactory<T> {

	private MessageHandlerMethodFactory handlerMethodFactory = new DefaultMessageHandlerMethodFactory();

	@Override
	public AsyncMessageListener<T> createFor(Endpoint<T> endpoint) {
		Assert.isInstanceOf(AbstractEndpoint.class, endpoint, "Endpoint must be an instance of AbstractEndpoint");
		Assert.notNull(this.handlerMethodFactory, "No handlerMethodFactory has been set");
		AbstractEndpoint<T> abstractEndpoint = (AbstractEndpoint<T>) endpoint;
		return new AsyncSingleMessageListenerAdapter<>(
			this.handlerMethodFactory.createInvocableHandlerMethod(abstractEndpoint.getBean(), abstractEndpoint.getMethod()));
	}

	public void setHandlerMethodFactory(MessageHandlerMethodFactory handlerMethodFactory) {
		Assert.notNull(handlerMethodFactory, "handlerMethodFactory cannot be null");
		this.handlerMethodFactory = handlerMethodFactory;
	}
}
