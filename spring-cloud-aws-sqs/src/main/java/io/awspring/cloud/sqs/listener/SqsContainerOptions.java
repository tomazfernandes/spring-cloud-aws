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
package io.awspring.cloud.sqs.listener;

import com.fasterxml.jackson.databind.util.BeanUtil;
import io.awspring.cloud.messaging.support.listener.AbstractContainerOptions;
import io.awspring.cloud.messaging.support.listener.AsyncMessageListener;
import org.springframework.beans.BeanUtils;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsContainerOptions extends AbstractContainerOptions<String, SqsContainerOptions> {

//	private final Map<String, QueueAttributes> queuesAttributes;

	private Integer minTimeToProcess;

//	private SqsContainerOptions(Map<String, QueueAttributes> queuesAttributes) {
//		this.queuesAttributes = queuesAttributes;
//	}
//

	private SqsContainerOptions() {
	}

	public static SqsContainerOptions create() {
		return new SqsContainerOptions();
	}
//
//	public static SqsContainerOptions create(Map<String, QueueAttributes> queuesAttributes) {
//		return new SqsContainerOptions(queuesAttributes);
//	}

	public SqsContainerOptions minTimeToProcess(Integer minTimeToProcess) {
		this.minTimeToProcess = minTimeToProcess;
		return this;
	}

	public Integer getMinTimeToProcess() {
		return minTimeToProcess;
	}
//
//	public Map<String, QueueAttributes> getQueuesAttributes() {
//		return queuesAttributes;
//	}

	@Override
	protected SqsContainerOptions doCreateCopy() {
		SqsContainerOptions newCopy = new SqsContainerOptions();
		ReflectionUtils.shallowCopyFieldState(this, newCopy);
		return newCopy;
	}
}
