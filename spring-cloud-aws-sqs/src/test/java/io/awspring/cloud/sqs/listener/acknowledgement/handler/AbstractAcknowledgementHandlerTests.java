package io.awspring.cloud.sqs.listener.acknowledgement.handler;

import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.acknowledgement.AcknowledgementCallback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.mockito.BDDMockito.given;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
@ExtendWith(MockitoExtension.class)
public class AbstractAcknowledgementHandlerTests {

	@Mock
	protected Message<String> message;

	protected Collection<Message<String>> messages;

	@Mock
	protected AcknowledgementCallback<String> callback;

	@Mock
	protected MessageHeaders headers;

	protected UUID id = UUID.randomUUID();

	@Mock
	protected Throwable throwable;

	@BeforeEach
	void beforeEach() {
		given(message.getHeaders()).willReturn(headers);
		given(headers.get(SqsHeaders.SQS_MESSAGE_ID_HEADER, UUID.class)).willReturn(id);
		messages = Collections.singletonList(message);
	}

}
