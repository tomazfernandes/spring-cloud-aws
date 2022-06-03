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
package io.awspring.cloud.sqs.config;

import io.awspring.cloud.messaging.support.config.MessagingConfigUtils;
import io.awspring.cloud.messaging.support.listener.AsyncMessageListener;
import io.awspring.cloud.sqs.annotation.EnableSqs;
import io.awspring.cloud.sqs.invocation.SqsEndpointMessageHandler;
import io.awspring.cloud.sqs.listener.MessageHandlerMessageListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.MessageHandler;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Creates the necessary beans.
 * Imported in the {@link EnableSqs @EnableSqs} annotation.
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsConfigurationSupport {

	@Bean(name = MessagingConfigUtils.MESSAGE_LISTENER_BEAN_NAME)
	public AsyncMessageListener<String> messageListener(MessageHandler messageHandler) {
		return new MessageHandlerMessageListener<>(messageHandler);
	}

	@Bean(name = MessagingConfigUtils.ENDPOINT_REGISTRY_BEAN_NAME)
	public MessageHandler endpointMessageHandler(
			@Qualifier(SqsConfigUtils.SQS_ASYNC_CLIENT_BEAN_NAME) SqsAsyncClient sqsAsyncClient) {
		return new SqsEndpointMessageHandler(sqsAsyncClient);
	}

}
