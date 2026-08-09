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
package io.awspring.cloud.autoconfigure;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The base contract for the integration tests of this module, providing a Localstack container shared by all of them.
 * <p>
 * Each test class used to declare its own container, so a run started one per class and paid its startup before any
 * assertion could be made. Localstack starts its services lazily on first use, so a single container serves all of
 * them. Classes are expected to keep their resource names to themselves, since they no longer get an empty account.
 *
 * @author Tomaz Fernandes
 */
@Testcontainers(disabledWithoutDocker = true)
public interface LocalstackContainerTest {

	LocalStackContainer LOCAL_STACK_CONTAINER = new LocalStackContainer(
			DockerImageName.parse("localstack/localstack:4.4.0"));

	@BeforeAll
	static void startContainer() {
		synchronized (LOCAL_STACK_CONTAINER) {
			LOCAL_STACK_CONTAINER.start();
		}
	}

}
