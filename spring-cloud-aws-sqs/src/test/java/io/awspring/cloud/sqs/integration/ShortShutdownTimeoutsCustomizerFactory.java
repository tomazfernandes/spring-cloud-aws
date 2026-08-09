/*
 * Copyright 2013-2026 the original author or authors.
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
package io.awspring.cloud.sqs.integration;

import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import java.time.Duration;
import java.util.List;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.springframework.test.context.MergedContextConfiguration;

/**
 * Shortens the listener and acknowledgement shutdown timeouts for every container factory in the test contexts of this
 * module.
 * <p>
 * Both default to twenty seconds, which suits an application draining in-flight work on shutdown. Tests stop their
 * containers with nothing in flight, so waiting that long only delays the suite.
 *
 * @author Tomaz Fernandes
 */
public class ShortShutdownTimeoutsCustomizerFactory implements ContextCustomizerFactory {

	// A zero timeout makes the sources skip draining and cancel their polling futures straight away. Anything
	// above zero is spent waiting for permits that in-flight polls only release when they return.
	private static final Duration SHUTDOWN_TIMEOUT = Duration.ZERO;

	// The suite's queues are served by one Localstack, so a long poll parks a request on it for its whole
	// duration. Shorten it so containers give the poll back and pick redelivered messages up sooner.
	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);

	@Override
	public ContextCustomizer createContextCustomizer(Class<?> testClass,
			List<ContextConfigurationAttributes> configAttributes) {
		return BaseSqsIntegrationTest.class.isAssignableFrom(testClass) ? new ShortShutdownTimeouts() : null;
	}

	private static class ShortShutdownTimeouts implements ContextCustomizer {

		@Override
		public void customizeContext(ConfigurableApplicationContext context, MergedContextConfiguration mergedConfig) {
			context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor() {
				@Override
				public Object postProcessBeforeInitialization(Object bean, String beanName) {
					if (bean instanceof SqsMessageListenerContainerFactory<?> factory) {
						factory.configure(options -> options.listenerShutdownTimeout(SHUTDOWN_TIMEOUT)
								.acknowledgementShutdownTimeout(SHUTDOWN_TIMEOUT).acknowledgementInterval(Duration.ZERO)
								.acknowledgementThreshold(0).pollTimeout(POLL_TIMEOUT));
					}
					return bean;
				}
			});
		}

		@Override
		public boolean equals(Object other) {
			return other instanceof ShortShutdownTimeouts;
		}

		@Override
		public int hashCode() {
			return ShortShutdownTimeouts.class.hashCode();
		}

	}

}
