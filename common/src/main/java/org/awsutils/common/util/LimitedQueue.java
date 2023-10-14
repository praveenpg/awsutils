package org.awsutils.common.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * This queue is used while creating an executorService, if backPressure functionality is required.
 * If the number of items added to the queue equals maxSize, any new addition will be blocked till
 * an element is removed.
 * @param <E>
 */
public final class LimitedQueue<E> extends LinkedBlockingQueue<E> {
    private ThreadPoolExecutor threadPoolExecutor;

    public LimitedQueue(final int maxSize) {
        super(maxSize);
    }

    @Override
    public boolean offer(final E e) {
        // turn offer() and add() into a blocking calls (unless interrupted)
        try {
            boolean returnValue = true;

            if(threadPoolExecutor != null) {
                final int poolSize = threadPoolExecutor.getPoolSize();
                final int maximumPoolSize = threadPoolExecutor.getMaximumPoolSize();

                if (poolSize < maximumPoolSize && poolSize <= threadPoolExecutor.getActiveCount()) {
                    returnValue = false;
                }
            }
            put(e);
            return returnValue;
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    public void setThreadPoolExecutor(final ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    public static class LimitedQueueRejectedExecutionPolicy implements RejectedExecutionHandler {
        public void rejectedExecution(final Runnable runnable, final ThreadPoolExecutor executor) {
        }
    }
}
