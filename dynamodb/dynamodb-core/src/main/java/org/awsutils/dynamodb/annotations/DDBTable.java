package org.awsutils.dynamodb.annotations;

import java.lang.annotation.*;

/**
 * Annotate the entity class with this annotation.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DDBTable {
    /**
     * Represents the DynamoDb table name
     * @return table name. If not populated, the class name is used as the table name. This can be constant string or a property name defined in config file.
     */
    String name() default "";
}
