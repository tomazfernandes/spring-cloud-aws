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

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

import com.amazonaws.auth.AWSCredentials;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

@Testcontainers
abstract class BaseSqsIntegrationTest {

	protected static final String RECEIVES_MESSAGE_QUEUE_NAME = "receives_message_test_queue";
	protected static final String DOES_NOT_ACK_ON_ERROR_QUEUE_NAME = "does_not_ack_test_queue";
	protected static final String RESOLVES_PARAMETER_TYPES_QUEUE_NAME = "resolves_parameter_test_queue";
	protected static final String RESOLVES_POJO_TYPES_QUEUE_NAME = "resolves_pojo_test_queue";
	protected static final String RECEIVE_FROM_MANY_1_QUEUE_NAME = "receive_many_test_queue_1";
	protected static final String RECEIVE_FROM_MANY_2_QUEUE_NAME = "receive_many_test_queue_2";
	protected static final String ASYNC_RECEIVE_FROM_MANY_1_QUEUE_NAME = "async_receive_many_test_queue_1";
	protected static final String ASYNC_RECEIVE_FROM_MANY_2_QUEUE_NAME = "async_receive_many_test_queue_2";

	@Container
	static LocalStackContainer localstack = new LocalStackContainer(
			DockerImageName.parse("localstack/localstack:0.14.0")).withServices(SQS).withReuse(false);

	static StaticCredentialsProvider credentialsProvider;

	@BeforeAll
	static void beforeAll() throws IOException, InterruptedException {
		// create needed queues in SQS
		// TODO: Not working as expected due to some port mapping issue - will look into in the future
		localstack.execInContainer("awslocal", "io/awspring/cloud/sqs", "create-queue", "--queue-name",
				RECEIVES_MESSAGE_QUEUE_NAME);
		localstack.execInContainer("awslocal", "io/awspring/cloud/sqs", "create-queue", "--queue-name",
				DOES_NOT_ACK_ON_ERROR_QUEUE_NAME);
		localstack.execInContainer("awslocal", "io/awspring/cloud/sqs", "create-queue", "--queue-name",
				RECEIVE_FROM_MANY_1_QUEUE_NAME);

		AWSCredentials localstackCredentials = localstack.getDefaultCredentialsProvider().getCredentials();
		credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials
				.create(localstackCredentials.getAWSAccessKeyId(), localstackCredentials.getAWSSecretKey()));
	}

	@DynamicPropertySource
	static void registerSqsProperties(DynamicPropertyRegistry registry) {
		// overwrite SQS endpoint with one provided by Localstack
		registry.add("spring.cloud.aws.endpoint", () -> localstack.getEndpointOverride(SQS).toString());
	}

}
