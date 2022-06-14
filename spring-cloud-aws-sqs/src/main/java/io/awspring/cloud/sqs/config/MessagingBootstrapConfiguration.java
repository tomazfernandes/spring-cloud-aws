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
