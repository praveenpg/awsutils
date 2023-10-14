package org.awsutils.dynamodb.annotations;

import java.lang.annotation.*;

/**
 * Annotate the entity field that represents the range key (sort key) with this annotation
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RangeKey {
}
