package org.awsutils.dynamodb.utils;

public interface Operator {
    String expression();

    Name name(String name, String alias);

    Group group(Expr expr);
}
