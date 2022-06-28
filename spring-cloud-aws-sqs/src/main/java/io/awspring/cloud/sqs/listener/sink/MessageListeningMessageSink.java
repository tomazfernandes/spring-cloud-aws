package io.awspring.cloud.sqs.listener.sink;

import io.awspring.cloud.sqs.listener.AsyncMessageListener;

public interface MessageListeningMessageSink<T> extends MessageSink<T> {

    void setMessageListener(AsyncMessageListener<T> listener);

}
