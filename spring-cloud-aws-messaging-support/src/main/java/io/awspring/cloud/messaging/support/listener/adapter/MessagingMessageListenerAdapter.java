package io.awspring.cloud.messaging.support.listener.adapter;

import io.awspring.cloud.messaging.support.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public abstract class MessagingMessageListenerAdapter {

	private final InvocableHandlerMethod handlerMethod;

	protected MessagingMessageListenerAdapter(InvocableHandlerMethod handlerMethod) {
		this.handlerMethod = handlerMethod;
	}

	protected final Object invokeHandler(Message<?> message) {

		try {
			return handlerMethod.invoke(message);
		} catch (Exception ex) {
			throw new ListenerExecutionFailedException("Listener failed to process message", ex);
		}

		// So maybe I could use the other HandlerMethod with a Message that has a List<POJO> payload
		// Or even a List<SQSMessage>
		// Still wouldn't be able to return CompletableFuture though, requiring that workaround
		// Also, to have a List<POJO> method argument, the MessageProducer would need to know beforehand the method argument, convert it, etc

		// So it's best I keep it flexible
		// the onMessage() methods specify whether it's a Message or List<Message>
		// In BatchListenerAdapter, SK converts each record to a message
		// and invokes the HandlerMethod with a List<Message>
		// In my case, I'll always and already receive the List<Message> at first
		// Then an adapter can split that if necessary
		// So what I need here is to be able to convert:
		//    	- a List<Message> into a List<POJO>
		//		- A Message into a POJO
		//		-

		// The Batch passes a Message with a List<Message>

		// So, what I need to figure out is whether I can use current strategy for handling Batches
		// Meaning, can I have a BatchArgumentProcessor

		// Problem is it's going to differ too much from SK

		// How'd that actually be?
		// Same as current, but I could have an adapter that will for example create a Message<List<POJO>>
		// While the other will iterate and call multiple times Message<POJO>

		// Problem is the endpoint might not know whether it's a batch or not
		// Or can it? It can because I have access to the method
		// Other batch related issues:
		// Can I infer that if I have a List<?> argument the method is a batch method?
		// In Spring Kafka users need to specify they want a batch listener
		//
		// So, do I still need separate interfaces for batch / non-batch?
		// Seems to me that I can use a List<Message<?>>
		// And depending on the container configuration / adapters,
		// I can either split the messages or send the list
		// I'll also need to add a converter for handling batches
		// Spring Kafka converts all messages into a Message<List<>>


		// What I'm looking for is how are converters invoked
		// At creation, the MessagingMethodListenerAdapter finds out
		// about custom arguments, including Lists
		// Fetches the type from Generics
		//
		//


		// Converters are invoked when


		// How about annotation processing?
		// Would there be any gain from having the BPP?


		// If so, I know there's a workaround to use CompletableFuture as return type
		// If that's simpler to build and maintain, might be worth it


	// So my question here is:
	// At this point, should I pass a Message<?> or an Object?
	// The difference is that with Object I can later on pass a List<Message>

	//


	}



}
