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
package io.awspring.cloud.sqs;

import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Utilities class for conditional configurations.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class ConfigUtils {

	public static final ConfigUtils INSTANCE = new ConfigUtils();

	private ConfigUtils() {
	}

	public <T> ConfigUtils acceptIfNotNull(T value, Consumer<T> consumer) {
		if (value != null) {
			consumer.accept(value);
		}
		return this;
	}

	public <T, V> ConfigUtils acceptBothIfNoneNull(T firstValue, V secondValue, BiConsumer<T, V> consumer) {
		if (firstValue != null && secondValue != null) {
			consumer.accept(firstValue, secondValue);
		}
		return this;
	}

	@SuppressWarnings("varargs")
	@SafeVarargs
	public final <T> ConfigUtils acceptFirstNonNull(Consumer<T> consumer, T... values) {
		Arrays.stream(values).filter(Objects::nonNull).findFirst().ifPresent(consumer);
		return this;
	}

	public <T> ConfigUtils acceptIfNotEmpty(Collection<T> value, Consumer<Collection<T>> consumer) {
		if (!CollectionUtils.isEmpty(value)) {
			consumer.accept(value);
		}
		return this;
	}
}
