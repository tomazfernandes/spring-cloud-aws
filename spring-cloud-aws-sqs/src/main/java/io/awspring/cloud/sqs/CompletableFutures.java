package io.awspring.cloud.sqs;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 *
 * Some utility methods from https://github.com/spotify/completable-futures.
 * Credit to the authors.
 *
 * @author Tomaz Fernandes
 * @since 3.0
 */
public class CompletableFutures {

	public static <T> CompletableFuture<T> failedFuture(Throwable t) {
		CompletableFuture<T> future = new CompletableFuture<>();
		future.completeExceptionally(t);
		return future;
	}

	public static <T> CompletableFuture<T> exceptionallyCompose(
		CompletableFuture<T> future,
		Function<Throwable, ? extends CompletableFuture<T>> fn) {
		return dereference(future.thenApply(CompletableFuture::completedFuture)
			.exceptionally(fn));
	}

	public static <T, U> CompletableFuture<U> handleCompose(
		CompletableFuture<T> stage,
		BiFunction<? super T, Throwable, ? extends CompletableFuture<U>> fn) {
		return dereference(stage.handle(fn));
	}

	public static <T> CompletableFuture<T> dereference(
		CompletableFuture<? extends CompletableFuture<T>> stage) {
		return stage.thenCompose(Function.identity());
	}
}
