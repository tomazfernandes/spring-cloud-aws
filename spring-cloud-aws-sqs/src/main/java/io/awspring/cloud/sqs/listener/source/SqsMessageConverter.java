//package io.awspring.cloud.sqs.listener.source;
//
//import io.awspring.cloud.sqs.listener.QueueMessageVisibility;
//import io.awspring.cloud.sqs.listener.SqsMessageHeaders;
//import io.awspring.cloud.sqs.listener.acknowledgement.SqsAcknowledge;
//import org.springframework.messaging.Message;
//import org.springframework.messaging.support.GenericMessage;
//import org.springframework.messaging.support.MessageBuilder;
//import org.springframework.util.MimeType;
//import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
//
//import java.time.Instant;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.UUID;
//
///**
// * @author Tomaz Fernandes
// * @since
// */
//public class SqsMessageConverter<T> {
//
//	public Message<T> convertMessage(final software.amazon.awssdk.services.sqs.model.Message message) {
//		logger.trace("Converting message {} to messaging message", message.messageId());
//		HashMap<String, Object> additionalHeaders = new HashMap<>();
//		additionalHeaders.put(SqsMessageHeaders.SQS_LOGICAL_RESOURCE_ID, getPollingEndpointName());
//		additionalHeaders.put(SqsMessageHeaders.RECEIVED_AT, Instant.now());
//		additionalHeaders.put(SqsMessageHeaders.SQS_CLIENT_HEADER, this.sqsAsyncClient);
//		additionalHeaders.put(SqsMessageHeaders.QUEUE_VISIBILITY, this.queueAttributes.getVisibilityTimeout());
//		additionalHeaders.put(SqsMessageHeaders.VISIBILITY,
//			new QueueMessageVisibility(this.sqsAsyncClient, this.queueUrl, message.receiptHandle()));
//		return createMessage(message, Collections.unmodifiableMap(additionalHeaders));
//	}
//
//	// TODO: Convert the message payload to type T
//	@SuppressWarnings("unchecked")
//	private Message<T> createMessage(software.amazon.awssdk.services.sqs.model.Message message,
//									 Map<String, Object> additionalHeaders) {
//
//		HashMap<String, Object> messageHeaders = new HashMap<>();
//		messageHeaders.put(SqsMessageHeaders.MESSAGE_ID_MESSAGE_ATTRIBUTE_NAME, message.messageId());
//		messageHeaders.put(SqsMessageHeaders.RECEIPT_HANDLE_MESSAGE_ATTRIBUTE_NAME, message.receiptHandle());
//		messageHeaders.put(SqsMessageHeaders.SOURCE_DATA_HEADER, message);
//		messageHeaders.put(SqsMessageHeaders.ACKNOWLEDGMENT_HEADER,
//			new SqsAcknowledge(this.sqsAsyncClient, this.queueUrl, message.receiptHandle()));
//		messageHeaders.putAll(additionalHeaders);
//		messageHeaders.putAll(getAttributesAsMessageHeaders(message));
//		messageHeaders.putAll(getMessageAttributesAsMessageHeaders(message));
//
//		// I could leverage JacksonConverter here to pass the payload, the headers, and return a message with POJO payload
//		// But the interface is kind of funny as it would require me to pass a Message only to get the deserialized payload
//		return MessageBuilder.createMessage((T) message.body(), new SqsMessageHeaders(messageHeaders));
//	}
//
//	// TODO: Review this logic using streams
//	private static Map<String, Object> getMessageAttributesAsMessageHeaders(
//		software.amazon.awssdk.services.sqs.model.Message message) {
//
//		Map<String, Object> messageHeaders = new HashMap<>();
//		messageHeaders.put(SqsMessageHeaders.SQS_GROUP_ID_HEADER,
//			message.attributes().get(MessageSystemAttributeName.MESSAGE_GROUP_ID));
//		for (Map.Entry<MessageSystemAttributeName, String> messageAttribute : message.attributes().entrySet()) {
//			if (org.springframework.messaging.MessageHeaders.CONTENT_TYPE.equals(messageAttribute.getKey().name())) {
//				messageHeaders.put(org.springframework.messaging.MessageHeaders.CONTENT_TYPE,
//					MimeType.valueOf(messageAttribute.getValue()));
//			}
//			else if (org.springframework.messaging.MessageHeaders.ID.equals(messageAttribute.getKey().name())) {
//				messageHeaders.put(org.springframework.messaging.MessageHeaders.ID,
//					UUID.fromString(messageAttribute.getValue()));
//			}
//			else {
//				messageHeaders.put(messageAttribute.getKey().name(), messageAttribute.getValue());
//			}
//		}
//		return Collections.unmodifiableMap(messageHeaders);
//	}
//
//	private static Map<String, Object> getAttributesAsMessageHeaders(
//		software.amazon.awssdk.services.sqs.model.Message message) {
//		Map<String, Object> messageHeaders = new HashMap<>();
//		for (Map.Entry<MessageSystemAttributeName, String> attributeKeyValuePair : message.attributes().entrySet()) {
//			messageHeaders.put(attributeKeyValuePair.getKey().name(), attributeKeyValuePair.getValue());
//		}
//		return messageHeaders;
//	}
//}
