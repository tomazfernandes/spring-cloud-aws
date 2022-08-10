package io.awspring.cloud.sqs;

import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Utility methods to handle {@link SmartLifecycle} hooks.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class LifecycleHandler {

	private static final LifecycleHandler INSTANCE = new LifecycleHandler();

	public static LifecycleHandler get() {
		return INSTANCE;
	}

	private final SimpleAsyncTaskExecutor executor;

	private boolean parallelLifecycle = true;

	private LifecycleHandler() {
		executor = new SimpleAsyncTaskExecutor();
		executor.setThreadNamePrefix("lifecycle-thread-");
	}

	public void setParallelLifecycle(boolean parallelLifecycle) {
		this.parallelLifecycle = parallelLifecycle;
	}

	/**
	 * Execute the provided action if the provided objects
	 * are {@link SmartLifecycle} instances.
	 * @param action the action.
	 * @param objects the objects.
	 */
	public void manageLifecycle(Consumer<SmartLifecycle> action, Object... objects) {
		Arrays.stream(objects).forEach(object -> {
			if (object instanceof SmartLifecycle) {
				action.accept((SmartLifecycle) object);
			}
			else if (object instanceof Collection) {
				if (this.parallelLifecycle) {
					CompletableFuture
						.allOf(((Collection<?>) object).stream()
							.map(obj -> CompletableFuture.runAsync(() -> manageLifecycle(action, obj), executor))
							.toArray(CompletableFuture[]::new))
						.join();
				}
				else {
					((Collection<?>) object).forEach(obj -> manageLifecycle(action, obj));
				}
			}
		});
	}

	/**
	 * Starts the provided objects that are a {@link SmartLifecycle} instance.
	 * @param objects the objects.
	 */
	public void start(Object... objects) {
		manageLifecycle(SmartLifecycle::start, objects);
	}

	/**
	 * Starts the provided objects that are a {@link SmartLifecycle} instance.
	 * @param objects the objects.
	 */
	public void stop(Object... objects) {
		manageLifecycle(SmartLifecycle::stop, objects);
	}

	public boolean isRunning(Object object) {
		if (object instanceof SmartLifecycle) {
			return ((SmartLifecycle) object).isRunning();
		}
		return true;
	}

}
