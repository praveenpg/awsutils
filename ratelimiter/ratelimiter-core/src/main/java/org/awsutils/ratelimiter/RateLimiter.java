package org.awsutils.ratelimiter;

import io.vavr.Function0;
import org.awsutils.common.exceptions.UtilsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public interface RateLimiter {
    Logger LOGGER = LoggerFactory.getLogger(RateLimiter.class);

    boolean hasExceededMaxRate();

    default <T> T execute(Supplier<T> func0) {
        if (!this.hasExceededMaxRate()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("RateLimiter Maximum Rate: " + this.getMaxRate());
            }

            return func0.get();
        } else {
            throw new UtilsException("UNKNOWN_ERROR", "Could not execute");
        }
    }

    default void execute(Runnable action0) {
        if (!this.hasExceededMaxRate()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("RateLimiter Maximum Rate: " + this.getMaxRate());
            }

            action0.run();
        } else {
            throw new UtilsException("UNKNOWN_ERROR", "Could not execute");
        }
    }

    int getMaxRate();

    String getRateLimiterName();
}
