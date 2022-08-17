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

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.config.SqsBootstrapConfiguration;
import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.ContainerOptions;
import io.awspring.cloud.sqs.listener.FifoSqsComponentFactory;
import io.awspring.cloud.sqs.listener.MessageDeliveryStrategy;
import io.awspring.cloud.sqs.listener.MessageListener;
import io.awspring.cloud.sqs.listener.MessageListenerContainer;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.SqsMessageListenerContainer;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementCallback;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementHandler;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.OnSuccessAcknowledgementHandler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.util.Assert;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
@SpringBootTest
class SqsFifoIntegrationTests extends BaseSqsIntegrationTest {

	private static final Logger logger = LoggerFactory.getLogger(SqsFifoIntegrationTests.class);

	static final String FIFO_RECEIVES_MESSAGES_IN_ORDER_QUEUE_NAME = "fifo_receives_messages_in_order.fifo";

	static final String FIFO_RECEIVES_MESSAGE_IN_ORDER_MANY_GROUPS_QUEUE_NAME = "fifo_receives_messages_in_order_many_groups.fifo";

	static final String FIFO_STOPS_PROCESSING_ON_ERROR_QUEUE_NAME = "fifo_stops_processing_on_error.fifo";

	static final String FIFO_STOPS_PROCESSING_ON_ACK_ERROR_ERROR_QUEUE_NAME = "fifo_stops_processing_on_ack_error.fifo";

	static final String FIFO_RECEIVES_BATCHES_MANY_GROUPS_QUEUE_NAME = "fifo_receives_batches_many_groups.fifo";

	static final String FIFO_MANUALLY_CREATE_CONTAINER_QUEUE_NAME = "fifo_manually_create_container_test_queue.fifo";

	static final String FIFO_MANUALLY_CREATE_FACTORY_QUEUE_NAME = "fifo_manually_create_factory_test_queue.fifo";

	static final String FIFO_MANUALLY_CREATE_BATCH_CONTAINER_QUEUE_NAME = "fifo_manually_create_batch_container_test_queue.fifo";

	static final String FIFO_MANUALLY_CREATE_BATCH_FACTORY_QUEUE_NAME = "fifo_manually_create_batch_factory_test_queue.fifo";

	private static final String TEST_SQS_ASYNC_CLIENT_BEAN_NAME = "testSqsAsyncClient";

	private static final String ERROR_ON_ACK_FACTORY = "errorOnAckFactory";

	@Autowired
	LatchContainer latchContainer;

	@Autowired
	@Qualifier(TEST_SQS_ASYNC_CLIENT_BEAN_NAME)
	SqsAsyncClient sqsAsyncClient;

	@Autowired
	ObjectMapper objectMapper;

	@Autowired(required = false)
	ReceivesMessageInOrderListener receivesMessageInOrderListener;

	@Autowired(required = false)
	ReceivesMessageInOrderManyGroupsListener receivesMessageInOrderManyGroupsListener;

	@Autowired(required = false)
	StopsOnErrorListener stopsOnErrorListener;

	@Autowired(required = false)
	ReceivesBatchesFromManyGroupsListener receivesBatchesFromManyGroupsListener;

	@Autowired
	LoadSimulator loadSimulator;

	@Autowired
	Settings settings;

	@Autowired
	MessagesContainer messagesContainer;

	@BeforeAll
	static void beforeTests() {
		SqsAsyncClient client = createAsyncClient();
		CompletableFuture.allOf(
				createFifoQueue(client, FIFO_RECEIVES_MESSAGES_IN_ORDER_QUEUE_NAME, getVisibilityAttribute("20")),
				createFifoQueue(client, FIFO_RECEIVES_MESSAGE_IN_ORDER_MANY_GROUPS_QUEUE_NAME),
				createFifoQueue(client, FIFO_STOPS_PROCESSING_ON_ERROR_QUEUE_NAME, getVisibilityAttribute("2")),
				createFifoQueue(client, FIFO_STOPS_PROCESSING_ON_ACK_ERROR_ERROR_QUEUE_NAME,
						getVisibilityAttribute("2")),
				createFifoQueue(client, FIFO_RECEIVES_BATCHES_MANY_GROUPS_QUEUE_NAME),
				createFifoQueue(client, FIFO_MANUALLY_CREATE_CONTAINER_QUEUE_NAME),
				createFifoQueue(client, FIFO_MANUALLY_CREATE_FACTORY_QUEUE_NAME),
				createFifoQueue(client, FIFO_MANUALLY_CREATE_BATCH_CONTAINER_QUEUE_NAME),
				createFifoQueue(client, FIFO_MANUALLY_CREATE_BATCH_FACTORY_QUEUE_NAME)).join();
	}

	private static Map<QueueAttributeName, String> getVisibilityAttribute(String value) {
		return Collections.singletonMap(QueueAttributeName.VISIBILITY_TIMEOUT, value);
	}

	private static class Settings implements SmartInitializingSingleton {

		@Autowired
		LoadSimulator loadSimulator;

		private final int messagesPerTest = 5;

		@Override
		public void afterSingletonsInstantiated() {
			loadSimulator.setLoadEnabled(false);
			loadSimulator.setBound(1000);
			loadSimulator.setRandom(true);
		}
	}

	@Test
	void receivesMessagesInOrder() throws Exception {
		latchContainer.receivesMessageLatch = new CountDownLatch(this.settings.messagesPerTest);
		String messageGroupId = UUID.randomUUID().toString();
		List<String> values = IntStream.range(0, this.settings.messagesPerTest).mapToObj(String::valueOf)
				.collect(toList());
		String queueUrl = fetchQueueUrl(FIFO_RECEIVES_MESSAGES_IN_ORDER_QUEUE_NAME);
		sendMessageTo(queueUrl, values, messageGroupId);
		assertThat(latchContainer.receivesMessageLatch.await(60, TimeUnit.SECONDS)).isTrue();
		assertThat(receivesMessageInOrderListener.receivedMessages).containsExactlyElementsOf(values);
	}

	@Test
	void receivesMessagesInOrderFromManyMessageGroups() throws Exception {
		latchContainer.receivesMessageManyGroupsLatch = new CountDownLatch(this.settings.messagesPerTest * 3);
		List<String> values = IntStream.range(0, this.settings.messagesPerTest).mapToObj(String::valueOf)
				.collect(toList());
		String messageGroupId1 = UUID.randomUUID().toString();
		String messageGroupId2 = UUID.randomUUID().toString();
		String messageGroupId3 = UUID.randomUUID().toString();
		String queueUrl = fetchQueueUrl(FIFO_RECEIVES_MESSAGE_IN_ORDER_MANY_GROUPS_QUEUE_NAME);
		sendMessageTo(queueUrl, values, messageGroupId1);
		sendMessageTo(queueUrl, values, messageGroupId2);
		sendMessageTo(queueUrl, values, messageGroupId3);
		assertThat(latchContainer.receivesMessageManyGroupsLatch.await(20, TimeUnit.SECONDS)).isTrue();
		assertThat(receivesMessageInOrderManyGroupsListener.receivedMessages.get(messageGroupId1))
				.containsExactlyElementsOf(values);
		assertThat(receivesMessageInOrderManyGroupsListener.receivedMessages.get(messageGroupId2))
				.containsExactlyElementsOf(values);
		assertThat(receivesMessageInOrderManyGroupsListener.receivedMessages.get(messageGroupId3))
				.containsExactlyElementsOf(values);
	}

	@Test
	void stopsProcessingAfterException() throws Exception {
		latchContainer.stopsProcessingOnErrorLatch1 = new CountDownLatch(4);
		latchContainer.stopsProcessingOnErrorLatch2 = new CountDownLatch(this.settings.messagesPerTest + 1);
		List<String> values = IntStream.range(0, this.settings.messagesPerTest).mapToObj(String::valueOf)
				.collect(toList());
		String messageGroupId = UUID.randomUUID().toString();
		String queueUrl = fetchQueueUrl(FIFO_STOPS_PROCESSING_ON_ERROR_QUEUE_NAME);
		sendMessageTo(queueUrl, values, messageGroupId);
		assertThat(latchContainer.stopsProcessingOnErrorLatch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(stopsOnErrorListener.receivedMessagesBeforeException)
				.containsExactlyElementsOf(values.stream().limit(4).collect(toList()));
		assertThat(latchContainer.stopsProcessingOnErrorLatch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(stopsOnErrorListener.receivedMessagesBeforeException)
				.containsExactlyElementsOf(values.stream().limit(4).collect(toList()));
		assertThat(stopsOnErrorListener.receivedMessagesAfterException)
				.containsExactlyElementsOf(values.subList(3, this.settings.messagesPerTest));
	}

	@Test
	void stopsProcessingAfterAckException() throws Exception {
		latchContainer.stopsProcessingOnAckErrorLatch1 = new CountDownLatch(4);
		latchContainer.stopsProcessingOnAckErrorLatch2 = new CountDownLatch(this.settings.messagesPerTest + 1);
		List<String> values = IntStream.range(0, this.settings.messagesPerTest).mapToObj(String::valueOf)
				.collect(toList());
		String messageGroupId = UUID.randomUUID().toString();
		String queueUrl = fetchQueueUrl(FIFO_STOPS_PROCESSING_ON_ACK_ERROR_ERROR_QUEUE_NAME);
		sendMessageTo(queueUrl, values, messageGroupId);
		assertThat(latchContainer.stopsProcessingOnAckErrorLatch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(messagesContainer.stopsProcessingOnAckErrorBeforeThrown)
				.containsExactlyElementsOf(values.stream().limit(4).collect(toList()));
		assertThat(latchContainer.stopsProcessingOnAckErrorLatch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(messagesContainer.stopsProcessingOnAckErrorBeforeThrown)
				.containsExactlyElementsOf(values.stream().limit(4).collect(toList()));
		assertThat(messagesContainer.stopsProcessingOnAckErrorAfterThrown)
				.containsExactlyElementsOf(values.subList(3, this.settings.messagesPerTest));
	}

	@Test
	void receivesBatchesManyGroups() throws Exception {
		latchContainer.receivesBatchManyGroupsLatch = new CountDownLatch(this.settings.messagesPerTest * 3);
		List<String> values = IntStream.range(0, this.settings.messagesPerTest).mapToObj(String::valueOf)
				.collect(toList());
		String messageGroupId1 = UUID.randomUUID().toString();
		String messageGroupId2 = UUID.randomUUID().toString();
		String messageGroupId3 = UUID.randomUUID().toString();
		String queueUrl = fetchQueueUrl(FIFO_RECEIVES_BATCHES_MANY_GROUPS_QUEUE_NAME);
		sendMessageTo(queueUrl, values, messageGroupId1);
		sendMessageTo(queueUrl, values, messageGroupId2);
		sendMessageTo(queueUrl, values, messageGroupId3);
		assertThat(latchContainer.receivesBatchManyGroupsLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(receivesBatchesFromManyGroupsListener.receivedMessages.get(messageGroupId1))
				.containsExactlyElementsOf(values);
		assertThat(receivesBatchesFromManyGroupsListener.receivedMessages.get(messageGroupId2))
				.containsExactlyElementsOf(values);
		assertThat(receivesBatchesFromManyGroupsListener.receivedMessages.get(messageGroupId3))
				.containsExactlyElementsOf(values);
	}

	@Test
	void manuallyCreatesContainer() throws Exception {
		String queueUrl = fetchQueueUrl(FIFO_MANUALLY_CREATE_CONTAINER_QUEUE_NAME);
		List<String> values = IntStream.range(0, this.settings.messagesPerTest).mapToObj(String::valueOf)
				.collect(toList());
		sendMessageTo(queueUrl, values, UUID.randomUUID().toString());
		assertThat(latchContainer.manuallyCreatedContainerLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(messagesContainer.manuallyCreatedContainerMessages).containsExactlyElementsOf(values);
	}

	@Test
	void manuallyCreatesBatchContainer() throws Exception {
		String queueUrl = fetchQueueUrl(FIFO_MANUALLY_CREATE_BATCH_CONTAINER_QUEUE_NAME);
		List<String> values = IntStream.range(0, this.settings.messagesPerTest).mapToObj(String::valueOf)
				.collect(toList());
		sendMessageTo(queueUrl, values, UUID.randomUUID().toString());
		assertThat(latchContainer.manuallyCreatedBatchContainerLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(messagesContainer.manuallyCreatedBatchContainerMessages).containsExactlyElementsOf(values);
	}

	@Test
	void manuallyCreatesFactory() throws Exception {
		String queueUrl = fetchQueueUrl(FIFO_MANUALLY_CREATE_FACTORY_QUEUE_NAME);
		List<String> values = IntStream.range(0, this.settings.messagesPerTest).mapToObj(String::valueOf)
				.collect(toList());
		sendMessageTo(queueUrl, values, UUID.randomUUID().toString());
		assertThat(latchContainer.manuallyCreatedFactoryLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(messagesContainer.manuallyCreatedFactoryMessages).containsExactlyElementsOf(values);
	}

	@Test
	void manuallyCreatesBatchFactory() throws Exception {
		String queueUrl = fetchQueueUrl(FIFO_MANUALLY_CREATE_BATCH_FACTORY_QUEUE_NAME);
		List<String> values = IntStream.range(0, this.settings.messagesPerTest).mapToObj(String::valueOf)
				.collect(toList());
		sendMessageTo(queueUrl, values, UUID.randomUUID().toString());
		assertThat(latchContainer.manuallyCreatedBatchFactoryLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(messagesContainer.manuallyCreatedBatchFactoryMessages).containsExactlyElementsOf(values);
	}

	static class ReceivesMessageInOrderListener {

		List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());

		@Autowired
		LatchContainer latchContainer;

		@Autowired
		LoadSimulator loadSimulator;

		@SqsListener(queueNames = FIFO_RECEIVES_MESSAGES_IN_ORDER_QUEUE_NAME)
		void listen(Message<String> message) {
			logger.debug("Received message with id {} and payload {} from ReceivesMessageInOrderListener",
					MessageHeaderUtils.getId(message), message.getPayload());
			loadSimulator.runLoad();
			receivedMessages.add(message.getPayload());
			latchContainer.receivesMessageLatch.countDown();
		}
	}

	static class ReceivesMessageInOrderManyGroupsListener {

		Map<String, List<String>> receivedMessages = new ConcurrentHashMap<>();

		@Autowired
		LatchContainer latchContainer;

		@Autowired
		LoadSimulator loadSimulator;

		@SqsListener(queueNames = FIFO_RECEIVES_MESSAGE_IN_ORDER_MANY_GROUPS_QUEUE_NAME)
		void listen(String message,
				@Header(SqsHeaders.MessageSystemAttribute.SQS_MESSAGE_GROUP_ID_HEADER) String groupId) {
			logger.debug("Received message in listener method: " + message);
			loadSimulator.runLoad();
			receivedMessages.computeIfAbsent(groupId, newGroupId -> Collections.synchronizedList(new ArrayList<>()))
					.add(message);
			latchContainer.receivesMessageManyGroupsLatch.countDown();
			logger.debug("Message {} processed.", message);
		}
	}

	static class StopsOnErrorListener {

		List<String> receivedMessagesBeforeException = Collections.synchronizedList(new ArrayList<>());

		List<String> receivedMessagesAfterException = Collections.synchronizedList(new ArrayList<>());

		AtomicBoolean hasThrown = new AtomicBoolean(false);

		@Autowired
		LatchContainer latchContainer;

		@Autowired
		LoadSimulator loadSimulator;

		@SqsListener(queueNames = FIFO_STOPS_PROCESSING_ON_ERROR_QUEUE_NAME, messageVisibilitySeconds = "2")
		void listen(String message) throws Exception {
			logger.debug("Received message in listener method: " + message);
			loadSimulator.runLoad(500);
			if (!hasThrown.get()) {
				this.receivedMessagesBeforeException.add(message);
			}
			else {
				this.receivedMessagesAfterException.add(message);
			}
			latchContainer.stopsProcessingOnErrorLatch1.countDown();
			latchContainer.stopsProcessingOnErrorLatch2.countDown();
			if (!hasThrown.get() && "3".equals(message)) {
				this.hasThrown.compareAndSet(false, true);
				throw new RuntimeException("Expected exception");
			}
		}
	}

	static class StopsOnAckErrorListener {

		@Autowired
		LatchContainer latchContainer;

		@SqsListener(queueNames = FIFO_STOPS_PROCESSING_ON_ACK_ERROR_ERROR_QUEUE_NAME, factory = ERROR_ON_ACK_FACTORY, messageVisibilitySeconds = "2")
		void listen(String message) {
			logger.debug("Received message in listener method: " + message);
			latchContainer.stopsProcessingOnAckErrorLatch1.countDown();
			latchContainer.stopsProcessingOnAckErrorLatch2.countDown();
		}
	}

	static class ReceivesBatchesFromManyGroupsListener {

		Map<String, List<String>> receivedMessages = new ConcurrentHashMap<>();

		@Autowired
		LatchContainer latchContainer;

		@SqsListener(queueNames = FIFO_RECEIVES_BATCHES_MANY_GROUPS_QUEUE_NAME, messageVisibilitySeconds = "20")
		void listen(List<Message<String>> messages) {
			String firstMessage = messages.iterator().next().getPayload();// Make sure we got the right type
			Assert.isTrue(MessageHeaderUtils
					.getHeader(messages, SqsHeaders.MessageSystemAttribute.SQS_MESSAGE_GROUP_ID_HEADER, String.class)
					.stream().distinct().count() == 1, "More than one message group returned in the same batch");
			String messageGroupId = messages.iterator().next().getHeaders()
					.get(SqsHeaders.MessageSystemAttribute.SQS_MESSAGE_GROUP_ID_HEADER, String.class);
			List<String> values = messages.stream().map(Message::getPayload).collect(toList());
			logger.trace("Started processing messages {} for group id {}", values, messageGroupId);
			receivedMessages.computeIfAbsent(messageGroupId, groupId -> Collections.synchronizedList(new ArrayList<>()))
					.addAll(values);
			messages.forEach(msg -> latchContainer.receivesBatchManyGroupsLatch.countDown());
			logger.trace("Finished processing messages {} for group id {}", values, messageGroupId);
		}
	}

	private void sendMessageTo(String queueUrl, List<String> messageBodies, String messageGroupId) {
		try {
			int batchSize = messageBodies.size() - 1;
			if (useLocalStackClient) {
				sendManyTo(batchSize, queueUrl, messageBodies, messageGroupId);
			}
			else {
				CompletableFuture.runAsync(() -> sendManyTo(batchSize, queueUrl, messageBodies, messageGroupId));
			}
		}
		catch (Exception e) {
			logger.error("Error sending messages to queue {}", queueUrl, e);
			throw (RuntimeException) e;
		}
	}

	private void sendManyTo(int batchSize, String queueUrl, List<String> messageBodies, String messageGroupId) {
		IntStream.range(0, (batchSize / 10) + 1).forEach(index -> doSendMessageTo(queueUrl,
				messageBodies.subList(index * 10, Math.min((index + 1) * 10, messageBodies.size())), messageGroupId));
	}

	private void doSendMessageTo(String queueUrl, List<String> messageBodies, String messageGroupId) {
		sqsAsyncClient.sendMessageBatch(req -> req
				.entries(messageBodies.stream().map(body -> createEntry(body, messageGroupId)).collect(toList()))
				.queueUrl(queueUrl).build()).join();
		logger.debug("Sent messages to queue {} with messageBody {}", queueUrl, messageBodies);
	}

	private SendMessageBatchRequestEntry createEntry(String body, String messageGroupId) {
		return SendMessageBatchRequestEntry.builder().messageBody(body).id(UUID.randomUUID().toString())
				.messageGroupId(messageGroupId).messageDeduplicationId(UUID.randomUUID().toString()).build();
	}

	private String fetchQueueUrl(String receivesMessageQueueName) throws InterruptedException, ExecutionException {
		return this.sqsAsyncClient.getQueueUrl(req -> req.queueName(receivesMessageQueueName)).get().queueUrl();
	}

	static class LatchContainer {

		final CountDownLatch manuallyCreatedContainerLatch = new CountDownLatch(5);
		final CountDownLatch manuallyCreatedFactoryLatch = new CountDownLatch(5);
		final CountDownLatch manuallyCreatedBatchContainerLatch = new CountDownLatch(5);
		final CountDownLatch manuallyCreatedBatchFactoryLatch = new CountDownLatch(5);

		// Lazily initialized
		CountDownLatch receivesMessageLatch = new CountDownLatch(1);
		CountDownLatch receivesMessageManyGroupsLatch = new CountDownLatch(1);
		CountDownLatch stopsProcessingOnErrorLatch1 = new CountDownLatch(3);
		CountDownLatch stopsProcessingOnErrorLatch2 = new CountDownLatch(1);
		CountDownLatch stopsProcessingOnAckErrorLatch1 = new CountDownLatch(1);
		CountDownLatch stopsProcessingOnAckErrorLatch2 = new CountDownLatch(1);
		CountDownLatch receivesBatchManyGroupsLatch = new CountDownLatch(1);

	}

	static class MessagesContainer {

		List<String> manuallyCreatedContainerMessages = Collections.synchronizedList(new ArrayList<>());
		List<String> manuallyCreatedBatchContainerMessages = Collections.synchronizedList(new ArrayList<>());
		List<String> manuallyCreatedFactoryMessages = Collections.synchronizedList(new ArrayList<>());
		List<String> manuallyCreatedBatchFactoryMessages = Collections.synchronizedList(new ArrayList<>());
		List<String> stopsProcessingOnAckErrorBeforeThrown = Collections.synchronizedList(new ArrayList<>());
		List<String> stopsProcessingOnAckErrorAfterThrown = Collections.synchronizedList(new ArrayList<>());

	}

	@Import(SqsBootstrapConfiguration.class)
	@Configuration
	static class SQSConfiguration {

		MessagesContainer messagesContainer = new MessagesContainer();

		@Bean
		public MessagesContainer messagesContainer() {
			return this.messagesContainer;
		}

		// @formatter:off
		@Bean
		public SqsMessageListenerContainerFactory<String> defaultSqsListenerContainerFactory() {
			SqsMessageListenerContainerFactory<String> factory = new SqsMessageListenerContainerFactory<>();
			factory.configure(options -> options
				.permitAcquireTimeout(Duration.ofSeconds(1))
				.pollTimeout(Duration.ofSeconds(3)));
			factory.setSqsAsyncClientSupplier(BaseSqsIntegrationTest::createAsyncClient);
			return factory;
		}

		@Bean(ERROR_ON_ACK_FACTORY)
		public SqsMessageListenerContainerFactory<String> errorOnAckSqsListenerContainerFactory() {
			SqsMessageListenerContainerFactory<String> factory = new SqsMessageListenerContainerFactory<>();
			factory.configure(options -> options
				.permitAcquireTimeout(Duration.ofSeconds(1))
				.pollTimeout(Duration.ofSeconds(3)));
			factory.setSqsAsyncClientSupplier(BaseSqsIntegrationTest::createAsyncClient);
			factory.setComponentFactory(new FifoSqsComponentFactory<String>() {
				@Override
				public AcknowledgementHandler<String> createAcknowledgementHandler(ContainerOptions options) {
					return new OnSuccessAcknowledgementHandler<String>() {

						final AtomicBoolean hasThrown = new AtomicBoolean(false);

						@Override
						public CompletableFuture<Void> onSuccess(Message<String> message,
								AcknowledgementCallback<String> callback) {
							if (!hasThrown.get()) {
								messagesContainer.stopsProcessingOnAckErrorBeforeThrown.add(message.getPayload());
							}
							else {
								messagesContainer.stopsProcessingOnAckErrorAfterThrown.add(message.getPayload());
							}
							if (message.getPayload().equals("3") && hasThrown.compareAndSet(false, true)) {
								return CompletableFutures.failedFuture(new RuntimeException("Expected acking error"));
							}
							return super.onSuccess(message, callback);
						}
					};
				}
			});
			return factory;
		}

		@Bean
		public MessageListenerContainer<String> manuallyCreatedContainer() {
			SqsMessageListenerContainer<String> container = new SqsMessageListenerContainer<>(createAsyncClient());
			container.configure(options -> options
						.permitAcquireTimeout(Duration.ofSeconds(1))
						.pollTimeout(Duration.ofSeconds(1)));
			container.setQueueNames(FIFO_MANUALLY_CREATE_CONTAINER_QUEUE_NAME);
			container.setMessageListener(msg -> {
				messagesContainer.manuallyCreatedContainerMessages.add(msg.getPayload());
				latchContainer.manuallyCreatedContainerLatch.countDown();
			});
			return container;
		}

		@Bean
		public MessageListenerContainer<String> manuallyCreatedBatchContainer() {
			SqsMessageListenerContainer<String> container = new SqsMessageListenerContainer<>(createAsyncClient());
			container.configure(options -> options
						.permitAcquireTimeout(Duration.ofSeconds(1))
						.pollTimeout(Duration.ofSeconds(1))
						.messageDeliveryStrategy(MessageDeliveryStrategy.BATCH));
			container.setQueueNames(FIFO_MANUALLY_CREATE_BATCH_CONTAINER_QUEUE_NAME);
			container.setMessageListener(new MessageListener<String>() {
				@Override
				public void onMessage(Message<String> message) {
					throw new UnsupportedOperationException();
				}

				@Override
				public void onMessage(Collection<Message<String>> messages) {
					messagesContainer.manuallyCreatedBatchContainerMessages
							.addAll(messages.stream().map(Message::getPayload).collect(toList()));
					messages.forEach(msg -> latchContainer.manuallyCreatedBatchContainerLatch.countDown());
				}
			});
			return container;
		}

		@Bean
		public SqsMessageListenerContainer<String> manuallyCreatedFactory() {
			SqsMessageListenerContainerFactory<String> factory = new SqsMessageListenerContainerFactory<>();
			factory.configure(options ->
				options.maxInflightMessagesPerQueue(10)
					.pollTimeout(Duration.ofSeconds(1))
					.messagesPerPoll(10)
					.permitAcquireTimeout(Duration.ofSeconds(1)));
			factory.setSqsAsyncClient(BaseSqsIntegrationTest.createAsyncClient());
			factory.setMessageListener(msg -> {
				logger.debug("Processed message {}", msg.getPayload());
				messagesContainer.manuallyCreatedFactoryMessages.add(msg.getPayload());
				latchContainer.manuallyCreatedFactoryLatch.countDown();
			});
			return factory.createContainer(FIFO_MANUALLY_CREATE_FACTORY_QUEUE_NAME);
		}

		@Bean
		public MessageListenerContainer<String> manuallyCreatedBatchFactory() {
			SqsMessageListenerContainerFactory<String> factory = new SqsMessageListenerContainerFactory<>();
			factory.configure(options -> options
				.maxInflightMessagesPerQueue(10)
				.pollTimeout(Duration.ofSeconds(1))
				.messagesPerPoll(10)
				.permitAcquireTimeout(Duration.ofSeconds(1))
				.messageDeliveryStrategy(MessageDeliveryStrategy.BATCH));
			factory.setSqsAsyncClient(BaseSqsIntegrationTest.createAsyncClient());
			factory.setMessageListener(new MessageListener<String>() {
				@Override
				public void onMessage(Message<String> message) {
					throw new UnsupportedOperationException();
				}

				@Override
				public void onMessage(Collection<Message<String>> messages) {
					messagesContainer.manuallyCreatedBatchFactoryMessages
							.addAll(messages.stream().map(Message::getPayload).collect(toList()));
					messages.forEach(msg -> latchContainer.manuallyCreatedBatchFactoryLatch.countDown());
				}
			});
			return factory.createContainer(FIFO_MANUALLY_CREATE_BATCH_FACTORY_QUEUE_NAME);
		}
		// @formatter:on

		@Bean
		ReceivesMessageInOrderListener receivesMessageInOrderListener() {
			return new ReceivesMessageInOrderListener();
		}

		@Bean
		ReceivesMessageInOrderManyGroupsListener receivesMessageInOrderManyGroupsListener() {
			return new ReceivesMessageInOrderManyGroupsListener();
		}

		@Bean
		StopsOnErrorListener stopsOnErrorListener() {
			return new StopsOnErrorListener();
		}

		@Bean
		StopsOnAckErrorListener stopsOnAckErrorListener() {
			return new StopsOnAckErrorListener();
		}

		@Bean
		ReceivesBatchesFromManyGroupsListener receiveBatchesFromManyGroupsListener() {
			return new ReceivesBatchesFromManyGroupsListener();
		}

		LatchContainer latchContainer = new LatchContainer();

		@Bean
		LatchContainer latchContainer() {
			return this.latchContainer;
		}

		@Bean
		LoadSimulator loadSimulator() {
			return new LoadSimulator();
		}

		@Bean
		Settings settings() {
			return new Settings();
		}

		@Bean
		ObjectMapper objectMapper() {
			return new ObjectMapper();
		}

		@Bean(name = TEST_SQS_ASYNC_CLIENT_BEAN_NAME)
		SqsAsyncClient sqsAsyncClientProducer() {
			return BaseSqsIntegrationTest.createAsyncClient();
		}

	}

}
