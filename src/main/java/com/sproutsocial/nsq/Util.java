package com.sproutsocial.nsq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class Util {

    public static final Charset US_ASCII = Charset.forName("US-ASCII");
    public static final Charset UTF_8 = Charset.forName("UTF-8");

    private static final Logger logger = LoggerFactory.getLogger(Util.class);

    public static ThreadFactory threadFactory(final String name) {
        final AtomicLong count = new AtomicLong();
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = Executors.defaultThreadFactory().newThread(runnable);
                thread.setName(name + "-" + Long.toString(count.getAndIncrement()));
                //doesn't catch everything, just for extra safety
                thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        logger.error("uncaught error", e);
                    }
                });
                return thread;
            }
        };
    }

    public static long clock() {
        return System.nanoTime() / 1000000;
    }

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        }
        catch (IOException e) {
            logger.warn("problem closing. {}", e.toString());
        }
    }

    public static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void cancel(ScheduledFuture task) {
        if (task != null) {
            task.cancel(false);
        }
    }

    //useful for Collections.synchronized... that can't be iterated over without synching
    public static <T> List<T> copy(Collection<T> col) {
        synchronized (col) {
            List<T> ret = new ArrayList<T>(col.size());
            ret.addAll(col);
            return ret;
        }
    }

    //below copied from Guava

    public static void checkNotNull(Object obj) {
        if (obj == null) {
            throw new NullPointerException();
        }
    }

    public static void checkArgument(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkState(boolean expression, String errorMessage) {
        if (!expression) {
            throw new IllegalStateException(errorMessage);
        }
    }

    public static boolean shutdownAndAwaitTermination(ExecutorService service, long timeout, TimeUnit unit) {
        long halfTimeoutNanos = unit.toNanos(timeout) / 2;
        // Disable new tasks from being submitted
        service.shutdown();
        try {
            // Wait for half the duration of the timeout for existing tasks to terminate
            if (!service.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)) {
                // Cancel currently executing tasks
                service.shutdownNow();
                // Wait the other half of the timeout for tasks to respond to being cancelled
                service.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS);
            }
        }
        catch (InterruptedException ie) {
            // Preserve interrupt status
            Thread.currentThread().interrupt();
            // (Re-)Cancel if current thread also interrupted
            service.shutdownNow();
        }
        return service.isTerminated();
    }

    public static <T> T firstNonNull(T first, T second) {
        return first != null ? first : second;
    }

    public static boolean equal(Object a, Object b) {
        return a == b || (a != null && a.equals(b));
    }

    public static int hashCode(Object... objects) {
        return Arrays.hashCode(objects);
    }

    public static boolean isNullOrEmpty(String string) {
        return string == null || string.isEmpty();
    }

}
