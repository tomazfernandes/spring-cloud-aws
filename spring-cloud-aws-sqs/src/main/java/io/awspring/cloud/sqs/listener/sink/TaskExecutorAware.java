package io.awspring.cloud.sqs.listener.sink;

import org.springframework.core.task.TaskExecutor;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public interface TaskExecutorAware {

	void setTaskExecutor(TaskExecutor taskExecutor);

}
