package org.awsutils.dynamodb.annotations;

import java.lang.annotation.*;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DbAttribute {
    String value() default "";
    boolean nullable() default true;
}
