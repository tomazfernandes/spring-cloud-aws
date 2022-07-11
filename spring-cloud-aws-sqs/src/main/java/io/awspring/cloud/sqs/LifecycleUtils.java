package io.awspring.cloud.sqs;

import org.springframework.context.SmartLifecycle;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

/**
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class LifecycleUtils {

	public static void manageLifecycle(Consumer<SmartLifecycle> action, Object... objects) {
		Arrays.stream(objects).forEach(object -> {
			if (object instanceof SmartLifecycle) {
				action.accept((SmartLifecycle) object);
			} else if (object instanceof Collection) {
				((Collection<?>) object).forEach(innerObject -> manageLifecycle(action, innerObject));
			}
		});
	}

	public static void start(Object... objects) {
		manageLifecycle(SmartLifecycle::start, objects);
	}

	public static void stop(Object... objects) {
		manageLifecycle(SmartLifecycle::stop, objects);
	}

}
