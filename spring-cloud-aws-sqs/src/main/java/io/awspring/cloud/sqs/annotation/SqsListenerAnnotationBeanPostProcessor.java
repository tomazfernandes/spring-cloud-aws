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
package io.awspring.cloud.sqs.annotation;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.ExpressionResolvingHelper;
import io.awspring.cloud.sqs.config.DefaultMessageListenerFactory;
import io.awspring.cloud.sqs.config.Endpoint;
import io.awspring.cloud.sqs.config.EndpointRegistrar;
import io.awspring.cloud.sqs.config.MessageListenerFactory;
import io.awspring.cloud.sqs.config.SqsEndpoint;
import io.awspring.cloud.sqs.listener.MessageHeaders;
import io.awspring.cloud.sqs.listener.SqsMessageHeaders;
import io.awspring.cloud.sqs.support.AsyncAcknowledgmentHandlerMethodArgumentResolver;
import io.awspring.cloud.sqs.support.SqsHeadersMethodArgumentResolver;
import io.awspring.cloud.sqs.support.SqsMessageMethodArgumentResolver;
import io.awspring.cloud.sqs.support.VisibilityHandlerMethodArgumentResolver;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.SimpleMessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.HeaderMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


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
public class SqsListenerAnnotationBeanPostProcessor
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
		Map<Method, SqsListener> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
			(MethodIntrospector.MetadataLookup<SqsListener>) method ->
				AnnotatedElementUtils.findMergedAnnotation(method, SqsListener.class));

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

	private SqsEndpoint createEndpointFromAnnotation(Object bean, Method method, SqsListener annotation) {
		SqsEndpoint endpoint = doCreateEndpointFromAnnotation(bean, method, annotation);
		endpoint.setBean(bean);
		endpoint.setMethod(method);
		endpoint.setMessageListenerFactory(this.messageListenerFactory);
		return endpoint;
	}

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
				defaultHandlerMethodFactory.setArgumentResolvers(createArgumentResolvers(
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

	private static final String GENERATED_ID_PREFIX = "io.awspring.cloud.sqs.sqsListenerEndpointContainer#";

	private final AtomicInteger counter = new AtomicInteger();

	protected SqsEndpoint doCreateEndpointFromAnnotation(Object bean, Method method, SqsListener sqsListenerAnnotation) {
		return SqsEndpoint.from(resolveDestinationNames(sqsListenerAnnotation.value()))
			.factoryBeanName(resolveExpression().asString(sqsListenerAnnotation.factory(), "factory"))
			.id(getEndpointId(sqsListenerAnnotation))
			.pollTimeoutSeconds(resolveExpression().asInteger(sqsListenerAnnotation.pollTimeoutSeconds(), "pollTimeoutSeconds"))
			.simultaneousPollsPerQueue(resolveExpression().asInteger(sqsListenerAnnotation.concurrentPollsPerContainer(), "concurrentPollsPerContainer"))
			.minTimeToProcess(resolveExpression().asInteger(sqsListenerAnnotation.minSecondsToProcess(), "minSecondsToProcess"))
			.async(CompletionStage.class.isAssignableFrom(method.getReturnType()))
//			.queuesAttributes(logicalEndpointNames.stream()
//				.collect(Collectors.toMap(name -> name, this::getQueueAttributes)))
			.build();
	}

	private String getEndpointId(SqsListener kafkaListener) {
		if (StringUtils.hasText(kafkaListener.id())) {
			return resolveExpression().asString(kafkaListener.id(), "id");
		}
		else {
			return GENERATED_ID_PREFIX + this.counter.getAndIncrement();
		}
	}

	private Set<String> resolveDestinationNames(String[] destinationNames) {
		return Arrays
			.stream(destinationNames)
			.map(destinationName -> resolveExpression().asString(destinationName, "queueNames"))
			.collect(Collectors.toSet());
	}

	protected List<HandlerMethodArgumentResolver> createArgumentResolvers(Collection<MessageConverter> messageConverters, ObjectMapper objectMapper) {
		return Arrays.asList(
			new SqsHeadersMethodArgumentResolver(),
			new AsyncAcknowledgmentHandlerMethodArgumentResolver(MessageHeaders.ACKNOWLEDGMENT_HEADER),
			new VisibilityHandlerMethodArgumentResolver(SqsMessageHeaders.VISIBILITY),
			new SqsMessageMethodArgumentResolver(),
			new HeaderMethodArgumentResolver(new GenericConversionService(), null),
			new MessageMethodArgumentResolver(messageConverters.isEmpty() ? new StringMessageConverter()
				: new CompositeMessageConverter(messageConverters)),
			new PayloadMethodArgumentResolver(createPayloadArgumentCompositeConverter(messageConverters, objectMapper))
		);
	}

	private CompositeMessageConverter createPayloadArgumentCompositeConverter(Collection<MessageConverter> messageConverters, ObjectMapper objectMapper) {
		List<MessageConverter> payloadArgumentConverters = new ArrayList<>(messageConverters);
		payloadArgumentConverters.add(getDefaultMappingJackson2MessageConverter(objectMapper));
		payloadArgumentConverters.add(new SimpleMessageConverter());
		return new CompositeMessageConverter(payloadArgumentConverters);
	}

	private MappingJackson2MessageConverter getDefaultMappingJackson2MessageConverter(ObjectMapper objectMapper) {
		MappingJackson2MessageConverter jacksonMessageConverter = new MappingJackson2MessageConverter();
		jacksonMessageConverter.setSerializedPayloadClass(String.class);
		jacksonMessageConverter.setStrictContentTypeMatch(false);
		if (objectMapper != null) {
			jacksonMessageConverter.setObjectMapper(objectMapper);
		}
		return jacksonMessageConverter;
	}

}
