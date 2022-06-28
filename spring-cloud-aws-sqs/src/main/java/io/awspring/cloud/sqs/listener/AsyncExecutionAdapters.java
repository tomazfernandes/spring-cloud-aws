package io.awspring.cloud.sqs.listener;


import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;


/**
 * Utility class for adapting blocking processes to asynchronous components
 * including error handling.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class AsyncExecutionAdapters {

	private AsyncExecutionAdapters(){}

	/**
	 * Executes the provided blocking process and returns a void completed future.
	 * @param blockingProcess the blocking process.
	 * @return the completed future.
	 */
	public static CompletableFuture<Void> adaptFromBlocking(Runnable blockingProcess) {
		try {
			blockingProcess.run();
			return CompletableFuture.completedFuture(null);
		} catch (Exception e) {
			CompletableFuture<Void> result = new CompletableFuture<>();
			result.completeExceptionally(e);
			return result;
		}
	}

	/**
	 * Executes the provided blocking process and returns a completed future with the process' result.
	 * @param blockingProcess the blocking process.
	 * @return the completed future with the process' result.
	 */
	public static <T> CompletableFuture<T> adaptFromBlocking(Supplier<T> blockingProcess) {
		try {
			return CompletableFuture.completedFuture(blockingProcess.get());
		} catch (Exception e) {
			CompletableFuture<T> result = new CompletableFuture<>();
			result.completeExceptionally(e);
			return result;
		}
	}
}
