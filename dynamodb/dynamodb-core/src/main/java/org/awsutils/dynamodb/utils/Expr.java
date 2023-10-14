package org.awsutils.dynamodb.utils;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

public interface Expr {
    String expression();
    Map<String, String> attributeNameMap();
    Map<String, AttributeValue> attributeValueMap();

    static FilterExpr builder() {
        return FilterExpr.getInstance();
    }
}
