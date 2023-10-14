package org.awsutils.dynamodb.annotations;

import java.lang.annotation.*;

/**
 * Annotate the entity field that represents the hash key (partition key) with this annotation
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface HashKey {
}
