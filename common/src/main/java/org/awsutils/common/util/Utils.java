package org.awsutils.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.awsutils.common.exceptions.UtilsException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.NumberFormat;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

@Slf4j
public class Utils {
    private static final int VISIBILITY_TIMEOUT = 60;
    private static final Long MAX_RETRY_INTERVAL_IN_SECONDS_VAL = 30L;
    private static final Long MAX_RETRY_INTERVAL_IN_SECONDS = TimeUnit.MINUTES.toSeconds(MAX_RETRY_INTERVAL_IN_SECONDS_VAL);
    private static final int MAXIMUM_NO_OF_RETRIES = 5;

    private static final int VISIBILITY_TIMEOUT_MULTIPLICATION_FACTOR = 2;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();

    private static final Thread SHUTDOWN_HOOK = new Thread();

    public static <X> List<X> constructListFromJson(Class<X> paramType, final String json) {
        return constructListFromJson(paramType, json, e -> new RuntimeException("Invalid Json", e));
    }

    public static <X> List<X> constructListFromJson(Class<X> paramType, final String json, final Function<Throwable, ? extends RuntimeException> exceptionFunc) {
        try {
            return OBJECT_MAPPER.readValue(json, OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, paramType));
        } catch (IOException e) {
            throw exceptionFunc.apply(e);
        }
    }

    public static <T> T constructFromJson(final Class<T> clazz, final String json) {
        return constructFromJson(clazz, json, e -> new RuntimeException("Invalid Json", e));
    }

    public static <T> T constructFromJson(final Class<T> clazz, final String json, final Function<Throwable, ? extends RuntimeException> exceptionFunc) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (final IOException e) {
            throw exceptionFunc.apply(e);
        }
    }

    public static <T> String constructJson(final T message) {
        return constructJson(message, e -> new RuntimeException("Error while constructing json", e));
    }

    public static <T> String constructJson(final T message, final Function<Throwable, ? extends RuntimeException> exceptionFunc) {
        try {
            return OBJECT_MAPPER.writeValueAsString(message);
        } catch (final JsonProcessingException e) {
            throw exceptionFunc.apply(e);
        }
    }

    public static <T, R> R convert(final T obj1, final Class<R> type) {
        return convert(obj1, type, OBJECT_MAPPER);
    }

    public static <T, R> List<R> convertToList(final T obj1, final Class<R> type) {
        return convertToList(obj1, type, OBJECT_MAPPER);
    }

    public static <T, R> R convert(final T obj1, final Class<R> type, final ObjectMapper mapper) {
        return mapper.convertValue(obj1, type);
    }

    public static <T, R> List<R> convertToList(final T obj1, final Class<R> type, final ObjectMapper mapper) {
        return OBJECT_MAPPER.convertValue(obj1, OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, type));
    }

    public static <T> T constructObject(final Class<T> clazz) {
        try {
            final Constructor<T> constructor = clazz.getDeclaredConstructor();

            constructor.setAccessible(true);

            return constructObject(constructor);
        } catch (final UtilsException e) {
            throw e;
        } catch (final Exception e) {
            throw new UtilsException("UNKNOWN_ERROR", e);
        }
    }

    public static <T> T constructObject(final Constructor<T> constructor) {
        try {
            return constructor.newInstance();
        } catch (final Exception e) {
            throw new UtilsException("INVALID_JSON", e);
        }
    }

    public static String getUnformattedNumber(final Number number) {
        final NumberFormat numberFormat = NumberFormat.getNumberInstance();

        numberFormat.setGroupingUsed(false);

        return numberFormat.format(number);
    }

    @SuppressWarnings("unchecked")
    public static <T> T invokeMethod(final Object obj, final String methodName) {
        try {
            final Method method = getMethod(obj.getClass(), methodName);

            return (T) method.invoke(obj);
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Object> convertArrayToList(final Class<?> arrayType, final Object value) {
        final int length = Array.getLength(value);
        final ImmutableList.Builder<Object> builder = ImmutableList.builder();

        IntStream.range(0, length).forEach(i -> builder.add(Array.get(value, i)));

        return builder.build();
    }

    public static void executeWithTransactionId(final Runnable func, final String transactionId) {
        final boolean transactionIDEmpty = StringUtils.isEmpty(transactionId);

        try {
            if(!transactionIDEmpty) {
                MDC.put("TRANSACTION_ID", transactionId);
            }

            func.run();
        } finally {
            if(!transactionIDEmpty) {
                MDC.remove("TRANSACTION_ID");
            }
        }
    }

    public static int calculateVisibilityTimeout(int retryNumber) {
        final int visibilityTimeoutInSeconds;

        if(retryNumber <= MAXIMUM_NO_OF_RETRIES) {
            visibilityTimeoutInSeconds = VISIBILITY_TIMEOUT * (int)Math.pow(VISIBILITY_TIMEOUT_MULTIPLICATION_FACTOR, retryNumber > 0 ?
                    (retryNumber - 1) : retryNumber);
        } else {
            visibilityTimeoutInSeconds = VISIBILITY_TIMEOUT * (int)Math.pow(VISIBILITY_TIMEOUT_MULTIPLICATION_FACTOR, MAXIMUM_NO_OF_RETRIES);
        }

        return (visibilityTimeoutInSeconds > MAX_RETRY_INTERVAL_IN_SECONDS) ? MAX_RETRY_INTERVAL_IN_SECONDS.intValue()
                : visibilityTimeoutInSeconds;
    }

    public static <T> Method getMethod(final Class<T> clazz, final String methodName, final Class<?>... params) {
        try {
            final Method method = clazz.getDeclaredMethod(methodName, params);

            method.setAccessible(true);

            return method;
        } catch (final Exception e) {
            throw new UtilsException("UNKNOWN_ERROR", e);
        }
    }

    public static <T> Constructor<T> getConstructor(final Class<T> clazz, final Class<?>... params) {
        return getConstructor(clazz, e -> {}, params);
    }

    public static <T> Constructor<T> getConstructor(final Class<T> clazz, final Consumer<? super Exception> errorHandleFunc, final Class<?>... params) {
        try {
            final Constructor<T> constructor = clazz.getDeclaredConstructor(params);

            constructor.setAccessible(true);

            return constructor;
        } catch (final Exception e) {
            errorHandleFunc.accept(e);
            throw new UtilsException("UNKNOWN_ERROR", e);
        }
    }

    public static <T> T executeUsingLock(final Lock lock, final Supplier<T> function) {
        lock.lock();

        try {
            return function.get();
        } finally {
            lock.unlock();
        }
    }

    public static void executeUsingLock(final Lock lock, final Runnable function) {
        lock.lock();

        try {
            function.run();
        } finally {
            lock.unlock();
        }
    }

    public static <T> T handleInterruptedException(final InterruptedException ex, Supplier<T> func) {
        if(!isJVMShuttingDown()) {
            log.error("Interrupted Exception: " + ex, ex);
            Thread.currentThread().interrupt();
            throw new UtilsException("UNKNOWN_ERROR", ex);
        } else {
            log.warn("JVM is shutting down....");

            return func.get();
        }
    }

    public static void handleInterruptedException(final InterruptedException ex, Runnable func) {
        if(!isJVMShuttingDown()) {
            log.error("Interrupted Exception: " + ex, ex);
            Thread.currentThread().interrupt();
            throw new UtilsException("UNKNOWN_ERROR", ex);
        } else {
            log.warn("JVM is shutting down....");

            func.run();
        }
    }

    public static boolean isJVMShuttingDown() {
        return executeUsingLock(LOCK.writeLock(), () -> {
            try {
                //Runtime will not allow adding a shutdown hook if it is in the process of shutting down
                Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);
                Runtime.getRuntime().removeShutdownHook(SHUTDOWN_HOOK);
            } catch (final IllegalStateException e) {
                //This is thrown only if the JVM is in the process of shutting down
                return true;
            }

            return false;
        });
    }

    public static void executeUsingSemaphore(final Semaphore semaphore, final long timeout, final TimeUnit timeUnit, final Runnable function) throws InterruptedException {
        final boolean lockAcquired = semaphore.tryAcquire(timeout, timeUnit);

        try {
            if(lockAcquired) {
                function.run();
            }
        } finally {
            if(lockAcquired) {
                semaphore.release();
            }
        }
    }

    public static void executeUsingSemaphore(final Semaphore semaphore, final Runnable function) {
        executeUsingSemaphore(semaphore, function, e -> new UtilsException("UNKNOWN_ERROR, Error when executing method: ", e));
    }

    public static void executeUsingSemaphore(final Semaphore semaphore, final Runnable function, final Function<Throwable, ? extends RuntimeException> exceptionFunc) {

        try {
            semaphore.acquire();
            function.run();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw exceptionFunc.apply(e);
        } finally {
            semaphore.release();
        }
    }

    public static <T> T executeUsingSemaphore(final Semaphore semaphore, final long timeout, final TimeUnit timeUnit, final Supplier<T> function) throws InterruptedException {
        return executeUsingSemaphore(semaphore, timeout, timeUnit, function, () -> new UtilsException("UNKNOWN_ERROR, Error when executing method: "));
    }

    public static <T> T executeUsingSemaphore(final Semaphore semaphore, final long timeout, final TimeUnit timeUnit, final Supplier<T> function, final Supplier<? extends RuntimeException> exceptionFunc) throws InterruptedException {
        final boolean lockAcquired = semaphore.tryAcquire(timeout, timeUnit);

        try {
            if(lockAcquired) {
                return function.get();
            } else {
                throw exceptionFunc.get();
            }
        } finally {
            if(lockAcquired) {
                semaphore.release();
            }
        }
    }

    public static <T> T executeUsingSemaphore(final Semaphore semaphore, final Supplier<T> function) {
        return executeUsingSemaphore(semaphore, function, e -> new UtilsException("UNKNOWN_ERROR, Error when executing function: " + e, e));
    }

    public static <T> T executeUsingSemaphore(final Semaphore semaphore, final Supplier<T> function, final Function<Throwable, ? extends RuntimeException> exceptionFunc) {
        try {
            semaphore.acquire();

            return function.get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw exceptionFunc.apply(e);
        } finally {
            semaphore.release();
        }
    }
}
