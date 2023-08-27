package org.awsutils.dynamodb.utils;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public interface SingleValueComparator extends Comparator {
    Value value(final String name, AttributeValue value);
}
