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
package io.awspring.cloud.messaging.support.annotation;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.messaging.support.ExpressionResolvingHelper;
import io.awspring.cloud.messaging.support.config.AbstractEndpoint;
import io.awspring.cloud.messaging.support.config.DefaultMessageListenerFactory;
import io.awspring.cloud.messaging.support.config.Endpoint;
import io.awspring.cloud.messaging.support.config.EndpointRegistrar;
import io.awspring.cloud.messaging.support.config.MessageListenerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


/**
 * {@link BeanPostProcessor} implementation that scans the bean
 * for an {@link Annotation}, extracts information
 * to an {@link Endpoint}, and delegates to an {@link EndpointRegistrar}.
 *
 * The Endpoint configuration / initialization is delegated to the subclasses.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractAnnotationBeanPostProcessor<A extends Annotation, E extends AbstractEndpoint>
	implements BeanPostProcessor, BeanFactoryAware, SmartInitializingSingleton {

	private final Collection<Class<?>> nonAnnotatedClasses = Collections.synchronizedSet(new HashSet<>());

	private final ExpressionResolvingHelper expressionResolvingHelper = new ExpressionResolvingHelper();

	private final EndpointRegistrar endpointRegistrar = new EndpointRegistrar();

	private final MessageListenerFactory<?> messageListenerFactory = new DefaultMessageListenerFactory<>();

	private BeanFactory beanFactory;

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

		if (this.nonAnnotatedClasses.contains(bean.getClass())) {
			return bean;
		}

		Class<?> targetClass = AopUtils.getTargetClass(bean);
		Map<Method, A> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
			(MethodIntrospector.MetadataLookup<A>) method ->
				AnnotatedElementUtils.findMergedAnnotation(method, getAnnotationType()));

		if (annotatedMethods.isEmpty()) {
			this.nonAnnotatedClasses.add(bean.getClass());
		}

		annotatedMethods
			.entrySet()
			.stream()
			.map(entry -> createEndpointFromAnnotation(bean, entry.getKey(), entry.getValue()))
			.forEach(this.endpointRegistrar::registerEndpoint);

		return bean;
	}

	private E createEndpointFromAnnotation(Object bean, Method method, A annotation) {
		E endpoint = doCreateEndpointFromAnnotation(bean, method, annotation);
		endpoint.setBean(bean);
		endpoint.setMethod(method);
		endpoint.setMessageListenerFactory(this.messageListenerFactory);
		return endpoint;
	}

	protected abstract E doCreateEndpointFromAnnotation(Object bean, Method method, A annotation);

	protected abstract Class<A> getAnnotationType();

	protected ExpressionResolvingHelper resolveExpression() {
		return this.expressionResolvingHelper;
	}

	@Override
	public void afterSingletonsInstantiated() {
		this.endpointRegistrar.setBeanFactory(this.beanFactory);
		// TODO: Add EndpointRegistrarCustomizer (not that name) interface
		initializeHandlerMethodFactory();
		this.endpointRegistrar.afterSingletonsInstantiated();
	}

	protected void initializeHandlerMethodFactory() {
		MessageHandlerMethodFactory handlerMethodFactory = this.endpointRegistrar.getMessageHandlerMethodFactory();
		if (this.messageListenerFactory instanceof DefaultMessageListenerFactory) {
			((DefaultMessageListenerFactory<?>) this.messageListenerFactory)
				.setHandlerMethodFactory(handlerMethodFactory);
		}
		if (handlerMethodFactory instanceof DefaultMessageHandlerMethodFactory) {
			try {
				DefaultMessageHandlerMethodFactory defaultHandlerMethodFactory =
					(DefaultMessageHandlerMethodFactory) handlerMethodFactory;
				defaultHandlerMethodFactory.setArgumentResolvers(doCreateArgumentResolvers(
						this.endpointRegistrar.getMessageConverters(), this.endpointRegistrar.getObjectMapper()));
				defaultHandlerMethodFactory.afterPropertiesSet();
			} catch (Exception e) {
				throw new IllegalArgumentException("Error initializing MessageHandlerMethodFactory", e);
			}
		}
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
		this.expressionResolvingHelper.setBeanFactory(beanFactory);
	}

	protected abstract List<HandlerMethodArgumentResolver> doCreateArgumentResolvers(
		Collection<MessageConverter> messageConverters, ObjectMapper objectMapper);
}
