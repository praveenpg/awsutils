package org.awsutils.dynamodb.utils;

public interface Value {
    Name and(String name, String alias);

    Operator and();

    Name and(String name);

    Name or(String name, String alias);

    Operator or();

    Name or(String name);

    Expr buildFilterExpression();
}
