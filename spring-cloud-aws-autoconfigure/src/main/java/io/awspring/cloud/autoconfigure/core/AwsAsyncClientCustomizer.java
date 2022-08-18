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
package io.awspring.cloud.autoconfigure.core;

import org.springframework.lang.Nullable;
import software.amazon.awssdk.awscore.client.builder.AwsAsyncClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

/**
 * @author Matej Nedić
 * @author Tomaz Fernandes
 * @since 3.0.0
 */
public interface AwsAsyncClientCustomizer<T extends AwsClientBuilder<?, ?> & AwsAsyncClientBuilder<?, ?>> {

	@Nullable
	default ClientOverrideConfiguration overrideConfiguration() {
		return null;
	}

	@Nullable
	default SdkAsyncHttpClient asyncHttpClient() {
		return null;
	}

	@Nullable
	default SdkAsyncHttpClient.Builder<?> asyncHttpClientBuilder() {
		return null;
	}

	static <V extends AwsClientBuilder<?, ?> & AwsAsyncClientBuilder<?, ?>> void apply (
			AwsAsyncClientCustomizer<V> configurer, V builder) {
		if (configurer.overrideConfiguration() != null) {
			builder.overrideConfiguration(configurer.overrideConfiguration());
		}
		if (configurer.asyncHttpClient() != null) {
			builder.httpClient(configurer.asyncHttpClient());
		}
		if (configurer.asyncHttpClientBuilder() != null) {
			builder.httpClientBuilder(configurer.asyncHttpClientBuilder());
		}
	}
}
