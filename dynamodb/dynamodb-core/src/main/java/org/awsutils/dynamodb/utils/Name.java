package org.awsutils.dynamodb.utils;

public interface Name {
    SingleValueComparator gt();

    SingleValueComparator lt() ;

    SingleValueComparator gte();

    SingleValueComparator lte();

    SingleValueComparator eq();

    SingleValueComparator ne();

    SingleValueComparator notExists();

    DoubleValueComparator between();
}
