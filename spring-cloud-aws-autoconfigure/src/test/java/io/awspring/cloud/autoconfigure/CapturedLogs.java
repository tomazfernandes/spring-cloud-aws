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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

/**
 * Collects what a single logger writes while a test runs.
 * <p>
 * Spring Boot's {@code CapturedOutput} replaces {@code System.out} for the whole JVM, so with test classes running
 * concurrently a test also sees whatever the others happen to print. Attaching an appender to the logger under test
 * keeps the assertions to the messages that test caused.
 *
 * @author Tomaz Fernandes
 */
public final class CapturedLogs implements AutoCloseable {

	private final Logger logger;

	private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

	private final Level previousLevel;

	private CapturedLogs(Class<?> loggingClass) {
		this.logger = (Logger) LoggerFactory.getLogger(loggingClass);
		this.previousLevel = this.logger.getLevel();
		this.appender.start();
		this.logger.addAppender(this.appender);
		this.logger.setLevel(Level.DEBUG);
	}

	/**
	 * Starts collecting the messages written by the logger of the given class.
	 */
	public static CapturedLogs of(Class<?> loggingClass) {
		return new CapturedLogs(loggingClass);
	}

	/**
	 * Whether any collected message contains the given text.
	 */
	public boolean contains(String text) {
		return this.appender.list.stream().anyMatch(event -> event.getFormattedMessage().contains(text));
	}

	@Override
	public void close() {
		this.logger.setLevel(this.previousLevel);
		this.logger.detachAppender(this.appender);
		this.appender.stop();
	}

}
