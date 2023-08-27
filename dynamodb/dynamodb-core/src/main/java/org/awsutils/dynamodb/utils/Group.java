package org.awsutils.dynamodb.utils;

public interface Group {
    Operator and();
    Operator or();
    String expression();

    Expr buildFilterExpression();
}
