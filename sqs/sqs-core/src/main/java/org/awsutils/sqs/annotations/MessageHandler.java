package org.awsutils.sqs.annotations;

import java.lang.annotation.*;

/**
 * This annotation marks a class which implements #SqsMessageHandler as Sqs Message Handler.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MessageHandler {
    /**
     * Any SqsMessage that has a type used in the annotation would automatically be assigned to the handler annotated.
     * @return Type
     */
    String messageType() default "";

    /**
     * Issue types for which retries are to be suppressed. This will only for exceptions which are a sub-class of
     * ServiceException
     * @return Issue types
     */
    String[] skipRetryFor() default {};

    /**
     * Exception types for which retries are to be suppressed.
     * @return Array of exception classes
     */
    Class<? extends Exception>[] skipRetryForExceptions() default {};
}
