package io.awspring.cloud.sqs.annotation;

import io.awspring.cloud.messaging.support.annotation.AbstractAnnotationBeanPostProcessor;
import io.awspring.cloud.sqs.endpoint.SqsEndpoint;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
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
public class SqsListenerAnnotationBeanPostProcessor extends AbstractAnnotationBeanPostProcessor<SqsListener, SqsEndpoint> {

	private static final String GENERATED_ID_PREFIX = "io.awspring.cloud.sqs.sqsListenerEndpointContainer#";

	private final AtomicInteger counter = new AtomicInteger();

	@Override
	protected SqsEndpoint createEndpointFromAnnotation(Object bean, Method method, SqsListener sqsListenerAnnotation) {
		return SqsEndpoint.from(resolveDestinationNames(sqsListenerAnnotation.value()))
			.factoryBeanName(resolveExpression().asString(sqsListenerAnnotation.factory(), "factory"))
			.id(getEndpointId(sqsListenerAnnotation))
			.pollTimeoutSeconds(resolveExpression().asInteger(sqsListenerAnnotation.pollTimeoutSeconds(), "pollTimeoutSeconds"))
			.simultaneousPollsPerQueue(resolveExpression().asInteger(sqsListenerAnnotation.concurrentPollsPerContainer(), "concurrentPollsPerContainer"))
			.minTimeToProcess(resolveExpression().asInteger(sqsListenerAnnotation.minSecondsToProcess(), "minSecondsToProcess"))
			.async(CompletionStage.class.isAssignableFrom(method.getReturnType()))
			.bean(bean)
			.method(method)
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
}
