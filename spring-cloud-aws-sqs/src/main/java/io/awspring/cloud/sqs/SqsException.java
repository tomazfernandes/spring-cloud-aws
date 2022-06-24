package io.awspring.cloud.sqs;

import org.springframework.core.NestedRuntimeException;

/**
 * Top-level exception for Sqs exceptions
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class SqsException extends NestedRuntimeException {

	public SqsException(String msg) {
		super(msg);
	}

	public SqsException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
