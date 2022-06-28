/*
 * Copyright 2013-2022 the original author or authors.
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
package io.awspring.cloud.autoconfigure.sqs;

import io.awspring.cloud.autoconfigure.core.AwsClientBuilderConfigurer;
import io.awspring.cloud.autoconfigure.core.AwsClientCustomizer;
import io.awspring.cloud.autoconfigure.core.CredentialsProviderAutoConfiguration;
import io.awspring.cloud.autoconfigure.core.RegionProviderAutoConfiguration;
import io.awspring.cloud.sqs.config.SqsBootstrapConfiguration;
import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.acknowledgement.AckHandler;
import io.awspring.cloud.sqs.listener.errorhandler.AsyncErrorHandler;
import io.awspring.cloud.sqs.listener.interceptor.AsyncMessageInterceptor;
import io.awspring.cloud.sqs.listener.sink.MessageSink;
import java.util.Collection;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.util.ReflectionUtils;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for SQS integration.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass({ SqsAsyncClient.class })
@EnableConfigurationProperties({ SqsProperties.class })
@Import(SqsBootstrapConfiguration.class)
@AutoConfigureAfter({ CredentialsProviderAutoConfiguration.class, RegionProviderAutoConfiguration.class })
@ConditionalOnProperty(name = "spring.cloud.aws.sqs.enabled", havingValue = "true", matchIfMissing = true)
public class SqsAutoConfiguration {

	@ConditionalOnMissingBean
	@Bean
	public SqsAsyncClient sqsAsyncClient(SqsProperties properties,
			AwsClientBuilderConfigurer awsClientBuilderConfigurer,
			ObjectProvider<AwsClientCustomizer<SqsAsyncClientBuilder>> configurer) {
		return awsClientBuilderConfigurer.configure(SqsAsyncClient.builder(), properties, configurer.getIfAvailable())
				.build();
	}

	@ConditionalOnMissingBean
	@Bean
	public SqsMessageListenerContainerFactory<Object> defaultSqsListenerContainerFactory(
			ObjectProvider<SqsAsyncClient> sqsAsyncClient, ObjectProvider<ContainerOptions> containerOptions,
			ObjectProvider<AsyncErrorHandler<Object>> errorHandler,
			ObjectProvider<Collection<AsyncMessageInterceptor<Object>>> interceptors,
			ObjectProvider<AsyncMessageInterceptor<Object>> interceptor,
			ObjectProvider<MessageSink<Object>> messageSplitter,
			ObjectProvider<AckHandler<Object>> ackHandler) {
		SqsMessageListenerContainerFactory<Object> factory = new SqsMessageListenerContainerFactory<>();
		containerOptions
				.ifAvailable(options -> ReflectionUtils.shallowCopyFieldState(options, factory.getContainerOptions()));
		sqsAsyncClient.ifAvailable(factory::setSqsAsyncClient);
		errorHandler.ifAvailable(factory::setErrorHandler);
		ackHandler.ifAvailable(factory::setAckHandler);
		interceptor.ifAvailable(factory::addMessageInterceptor);
		interceptors.ifAvailable(factory::addMessageInterceptors);
		messageSplitter.ifAvailable(factory::setMessageSink);
		return factory;
	}
}
