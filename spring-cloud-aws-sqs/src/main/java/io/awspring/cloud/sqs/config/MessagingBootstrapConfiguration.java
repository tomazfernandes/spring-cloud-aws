package io.awspring.cloud.sqs.config;

import io.awspring.cloud.messaging.support.config.EndpointRegistrar;
import io.awspring.cloud.messaging.support.config.MessagingConfigUtils;
import io.awspring.cloud.messaging.support.listener.DefaultListenerContainerRegistry;
import io.awspring.cloud.sqs.annotation.SqsListenerAnnotationBeanPostProcessor;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 *
 * Registers the {@link DefaultListenerContainerRegistry} and
 * {@link EndpointRegistrar} that will be used to bootstrap
 * the framework.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class MessagingBootstrapConfiguration implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		if (!registry.containsBeanDefinition(MessagingConfigUtils.SQS_LISTENER_ANNOTATION_BEAN_POST_PROCESSOR_BEAN_NAME)) {
			registry.registerBeanDefinition(MessagingConfigUtils.SQS_LISTENER_ANNOTATION_BEAN_POST_PROCESSOR_BEAN_NAME,
					new RootBeanDefinition(SqsListenerAnnotationBeanPostProcessor.class));
		}

		if (!registry.containsBeanDefinition(MessagingConfigUtils.ENDPOINT_REGISTRY_BEAN_NAME)) {
			registry.registerBeanDefinition(MessagingConfigUtils.ENDPOINT_REGISTRY_BEAN_NAME,
					new RootBeanDefinition(DefaultListenerContainerRegistry.class));
		}
	}

}
