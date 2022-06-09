package io.awspring.cloud.messaging.support.annotation;

import io.awspring.cloud.messaging.support.ExpressionResolvingHelper;
import io.awspring.cloud.messaging.support.config.MessagingConfigUtils;
import io.awspring.cloud.messaging.support.endpoint.Endpoint;
import io.awspring.cloud.messaging.support.endpoint.EndpointRegistrar;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;


/**
 * {@link BeanPostProcessor} implementation that scans the bean
 * for an {@link Annotation} type, extracts information
 * to an {@link Endpoint}, and delegates to an {@link EndpointProcessor}.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class AbstractAnnotationBeanPostProcessor<A extends Annotation, E extends Endpoint>
	implements BeanPostProcessor, BeanFactoryAware, SmartInitializingSingleton {

	private final Collection<Class<?>> nonAnnotatedClasses = Collections.synchronizedSet(new HashSet<>());

	private final ExpressionResolvingHelper expressionResolvingHelper = new ExpressionResolvingHelper();

	private final EndpointRegistrar endpointRegistrar = new EndpointRegistrar();

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

	protected abstract E createEndpointFromAnnotation(Object bean, Method method, A annotation);

	protected abstract Class<A> getAnnotationType();

	protected ExpressionResolvingHelper resolveExpression() {


		return this.expressionResolvingHelper;
	}

	@Override
	public void afterSingletonsInstantiated() {
		this.endpointRegistrar.setBeanFactory(this.beanFactory);
		// TODO: Add EndpointRegistrarCustomizer interface
		this.endpointRegistrar.afterSingletonsInstantiated();

	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
		this.expressionResolvingHelper.setBeanFactory(beanFactory);
	}
}
