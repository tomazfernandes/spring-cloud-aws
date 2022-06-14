package io.awspring.cloud.sqs.annotation;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.messaging.support.annotation.AbstractAnnotationBeanPostProcessor;
import io.awspring.cloud.messaging.support.listener.MessageHeaders;
import io.awspring.cloud.sqs.config.SqsEndpoint;
import io.awspring.cloud.sqs.listener.SqsMessageHeaders;
import io.awspring.cloud.sqs.support.AsyncAcknowledgmentHandlerMethodArgumentResolver;
import io.awspring.cloud.sqs.support.SqsHeadersMethodArgumentResolver;
import io.awspring.cloud.sqs.support.SqsMessageMethodArgumentResolver;
import io.awspring.cloud.sqs.support.VisibilityHandlerMethodArgumentResolver;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.SimpleMessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.HeaderMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.MessageMethodArgumentResolver;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * A {@link AbstractAnnotationBeanPostProcessor} for processing
 * {@link SqsListener @SqsListener} annotations and creating {@link SqsEndpoint} instances
 * for a given bean instance.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsListenerAnnotationBeanPostProcessor extends AbstractAnnotationBeanPostProcessor<String, SqsListener, SqsEndpoint> {

	private static final String GENERATED_ID_PREFIX = "io.awspring.cloud.sqs.sqsListenerEndpointContainer#";

	private final AtomicInteger counter = new AtomicInteger();

	@Override
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

	@Override
	protected Class<SqsListener> getAnnotationType() {
		return SqsListener.class;
	}

	@Override
	protected void initializeHandlerMethodFactory(DefaultMessageHandlerMethodFactory handlerMethodFactory,
												  Collection<MessageConverter> messageConverters, ObjectMapper objectMapper) {
		handlerMethodFactory.setArgumentResolvers(createArgumentResolvers(messageConverters, objectMapper));
		handlerMethodFactory.afterPropertiesSet();
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
