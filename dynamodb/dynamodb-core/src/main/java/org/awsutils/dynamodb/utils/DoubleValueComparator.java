package org.awsutils.dynamodb.utils;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface DoubleValueComparator extends Comparator {
    Value value(String name1, AttributeValue value1, String name2, AttributeValue value2);
}
